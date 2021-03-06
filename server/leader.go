package server

import (
	"os"
	"path"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	storagepb "github.com/coreos/etcd/storage/storagepb"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// IsLeader returns whether server is leader or not.
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeader) == 1
}

func (s *Server) enableLeader(b bool) {
	value := int64(0)
	if b {
		value = 1
	}

	atomic.StoreInt64(&s.isLeader, value)

	if !b {
		// if we lost leader, we may:
		//	1, close all client connections
		//	2, close all running raft clusters
		s.closeAllConnections()

		s.closeClusters()
	}
}

// GetLeaderPath returns the leader path.
func GetLeaderPath(rootPath string) string {
	return path.Join(rootPath, "leader")
}

func (s *Server) getLeaderPath() string {
	return GetLeaderPath(s.cfg.RootPath)
}

func (s *Server) leaderLoop() {
	defer func() {
		s.wg.Done()
		s.enableLeader(false)
	}()

	for {
		if s.IsClosed() {
			return
		}

		s.enableLeader(false)

		leader, err := s.getLeader()
		if err != nil {
			log.Errorf("get leader err %v", err)
			continue
		} else if leader != nil {
			log.Debugf("leader is %s, watch it", leader)

			s.watchLeader()

			log.Debug("leader changed, try to campaign leader")
		}

		if err = s.campaignLeader(); err != nil {
			log.Errorf("campaign leader err %s", err)
		}
	}
}

// GetLeader gets server leader from etcd.
func GetLeader(c *clientv3.Client, leaderPath string) (*pdpb.Leader, error) {
	leader := pdpb.Leader{}
	ok, err := getProtoMsg(c, leaderPath, &leader)
	if err != nil || !ok {
		return nil, errors.Trace(err)
	}

	return &leader, nil
}

func (s *Server) getLeader() (*pdpb.Leader, error) {
	return GetLeader(s.client, s.getLeaderPath())
}

func (s *Server) marshalLeader() string {
	leader := &pdpb.Leader{
		Addr: proto.String(s.cfg.Addr),
		Pid:  proto.Int64(int64(os.Getpid())),
	}

	data, err := proto.Marshal(leader)
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("marshal leader %s err %v", leader, err)
	}

	return string(data)
}

func (s *Server) campaignLeader() error {
	log.Debug("begin to campaign leader")

	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	leaseResp, err := lessor.Create(ctx, s.cfg.LeaderLease)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	leaderKey := s.getLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, s.leaderValue, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	log.Debug("campaign leader ok")
	s.enableLeader(true)
	defer s.enableLeader(false)

	// keeps the leader
	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("sync timestamp for tso")
	if err = s.syncTimestamp(); err != nil {
		return errors.Trace(err)
	}

	tsTicker := time.NewTicker(time.Duration(updateTimestampStep) * time.Millisecond)
	defer tsTicker.Stop()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("keep alive channel is closed")
				return nil
			}
		case <-tsTicker.C:
			if err = s.updateTimestamp(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (s *Server) watchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	for {
		rch := watcher.Watch(s.client.Ctx(), s.getLeaderPath())
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == storagepb.EXPIRE || ev.Type == storagepb.DELETE {
					log.Info("leader is expired or deleted")
					return
				}
			}
		}
	}
}

func (s *Server) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.getLeaderPath()), "=", s.leaderValue)
}
