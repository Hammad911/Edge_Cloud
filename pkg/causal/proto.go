package causal

import (
	replv1 "edge-cloud-replication/gen/proto/edgecloud/replication/v1"
	"edge-cloud-replication/pkg/hlc"
)

// EventToProto encodes an in-memory Event for the wire.
func EventToProto(e *Event) *replv1.Event {
	return &replv1.Event{
		EventId:  e.EventID,
		SenderId: e.SenderID,
		Origin:   string(e.Origin),
		Key:      e.Key,
		Value:    e.Value,
		Deleted:  e.Deleted,
		CommitTs: tsToProto(e.CommitTS),
		Deps:     partitionedToProto(e.Deps),
	}
}

// EventFromProto decodes a wire Event into the in-memory shape.
func EventFromProto(p *replv1.Event) *Event {
	return &Event{
		EventID:  p.GetEventId(),
		SenderID: p.GetSenderId(),
		Origin:   hlc.GroupID(p.GetOrigin()),
		Key:      p.GetKey(),
		Value:    cloneBytes(p.GetValue()),
		Deleted:  p.GetDeleted(),
		CommitTS: tsFromProto(p.GetCommitTs()),
		Deps:     partitionedFromProto(p.GetDeps()),
	}
}

func tsToProto(t hlc.Timestamp) *replv1.HLCTimestamp {
	return &replv1.HLCTimestamp{Physical: t.Physical, Logical: t.Logical}
}

func tsFromProto(p *replv1.HLCTimestamp) hlc.Timestamp {
	if p == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{Physical: p.GetPhysical(), Logical: p.GetLogical()}
}

func partitionedToProto(p hlc.PartitionedTimestamp) *replv1.PartitionedTimestamp {
	out := &replv1.PartitionedTimestamp{
		Origin: string(p.Origin),
		Groups: make([]*replv1.GroupClock, 0, len(p.Groups)),
	}
	for g, t := range p.Groups {
		out.Groups = append(out.Groups, &replv1.GroupClock{
			Group: string(g),
			Ts:    tsToProto(t),
		})
	}
	return out
}

func partitionedFromProto(p *replv1.PartitionedTimestamp) hlc.PartitionedTimestamp {
	if p == nil {
		return hlc.NewPartitionedTimestamp("")
	}
	out := hlc.PartitionedTimestamp{
		Origin: hlc.GroupID(p.GetOrigin()),
		Groups: make(map[hlc.GroupID]hlc.Timestamp, len(p.GetGroups())),
	}
	for _, g := range p.GetGroups() {
		out.Groups[hlc.GroupID(g.GetGroup())] = tsFromProto(g.GetTs())
	}
	return out
}
