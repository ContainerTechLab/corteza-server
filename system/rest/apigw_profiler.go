package rest

import (
	"context"

	"github.com/cortezaproject/corteza-server/pkg/filter"
	"github.com/cortezaproject/corteza-server/system/rest/request"
	"github.com/cortezaproject/corteza-server/system/service"
	"github.com/cortezaproject/corteza-server/system/types"
	"github.com/davecgh/go-spew/spew"
)

type (
	profilerService interface {
		Hits(context.Context, types.ApigwProfilerFilter) (types.ApigwProfilerAggregationSet, types.ApigwProfilerFilter, error)
	}

	ApigwProfiler struct {
		svc profilerService
		ac  templateAccessController
	}

	profilerHitPayload struct {
		*types.ApigwProfilerAggregation
	}

	profilerHitSetPayload struct {
		Filter types.ApigwProfilerFilter `json:"filter"`
		Set    []*profilerHitPayload     `json:"set"`
	}
)

func (ApigwProfiler) New() *ApigwProfiler {
	return &ApigwProfiler{
		svc: service.DefaultApigwRoute,
		ac:  service.DefaultAccessControl,
	}
}

func (ctrl *ApigwProfiler) List(ctx context.Context, r *request.ApigwProfilerList) (interface{}, error) {
	var (
		err error
		f   = types.ApigwProfilerFilter{
			Path:   r.Path,
			Before: r.Before,
		}
	)

	if f.Sorting, err = filter.NewSorting(r.Sort); err != nil {
		return nil, err
	}

	set, f, err := ctrl.svc.Hits(ctx, f)

	return ctrl.makeFilterPayload(ctx, set, f, err)
}

func (ctrl *ApigwProfiler) Aggregated(ctx context.Context, r *request.ApigwProfilerList) (interface{}, error) {
	var (
		// err error
		f = types.ApigwProfilerFilter{
			Path:   r.Path,
			Before: r.Before,
		}
	)

	spew.Dump("FILTER", f)

	// if f.Paging, err = filter.NewPaging(r.Limit, r.PageCursor); err != nil {
	// 	return nil, err
	// }

	// if f.Sorting, err = filter.NewSorting(r.Sort); err != nil {
	// 	return nil, err
	// }

	set, f, err := ctrl.svc.Hits(ctx, f)

	// spew.Dump("SET", set, f)

	return ctrl.makeFilterPayload(ctx, set, f, err)
}

func (ctrl *ApigwProfiler) makePayload(ctx context.Context, q *types.ApigwProfilerAggregation, err error) (*profilerHitPayload, error) {
	if err != nil || q == nil {
		return nil, err
	}

	qq := &profilerHitPayload{
		ApigwProfilerAggregation: q,
	}

	return qq, nil
}

func (ctrl *ApigwProfiler) makeFilterPayload(ctx context.Context, nn types.ApigwProfilerAggregationSet, f types.ApigwProfilerFilter, err error) (*profilerHitSetPayload, error) {
	if err != nil {
		return nil, err
	}

	msp := &profilerHitSetPayload{Filter: f, Set: make([]*profilerHitPayload, len(nn))}

	for i := range nn {
		msp.Set[i], _ = ctrl.makePayload(ctx, nn[i], nil)
	}

	return msp, nil
}
