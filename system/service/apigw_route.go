package service

import (
	"context"
	"encoding/base64"
	"math"
	"time"

	"github.com/cortezaproject/corteza-server/pkg/actionlog"
	"github.com/cortezaproject/corteza-server/pkg/apigw"
	"github.com/cortezaproject/corteza-server/pkg/apigw/profiler"
	a "github.com/cortezaproject/corteza-server/pkg/auth"

	"github.com/cortezaproject/corteza-server/store"
	"github.com/cortezaproject/corteza-server/system/types"
)

type (
	apigwRoute struct {
		actionlog actionlog.Recorder
		store     store.Storer
		ac        routeAccessController
	}

	routeAccessController interface {
		CanGrant(context.Context) bool
		CanSearchApigwRoutes(ctx context.Context) bool

		CanCreateApigwRoute(context.Context) bool
		CanReadApigwRoute(context.Context, *types.ApigwRoute) bool
		CanUpdateApigwRoute(context.Context, *types.ApigwRoute) bool
		CanDeleteApigwRoute(context.Context, *types.ApigwRoute) bool
	}
)

func Route() *apigwRoute {
	return &apigwRoute{
		ac:        DefaultAccessControl,
		actionlog: DefaultActionlog,
		store:     DefaultStore,
	}
}

func (svc *apigwRoute) FindByID(ctx context.Context, ID uint64) (q *types.ApigwRoute, err error) {
	var (
		rProps = &apigwRouteActionProps{}
	)

	err = func() error {
		if ID == 0 {
			return ApigwRouteErrInvalidID()
		}

		if q, err = store.LookupApigwRouteByID(ctx, svc.store, ID); err != nil {
			return ApigwRouteErrInvalidID().Wrap(err)
		}

		rProps.setRoute(q)

		if !svc.ac.CanReadApigwRoute(ctx, q) {
			return ApigwRouteErrNotAllowedToRead(rProps)
		}

		return nil
	}()

	return q, svc.recordAction(ctx, rProps, ApigwRouteActionLookup, err)
}

func (svc *apigwRoute) Create(ctx context.Context, new *types.ApigwRoute) (q *types.ApigwRoute, err error) {
	var (
		qProps = &apigwRouteActionProps{new: new}
	)

	err = func() (err error) {
		if !svc.ac.CanCreateApigwRoute(ctx) {
			return ApigwRouteErrNotAllowedToCreate(qProps)
		}

		new.ID = nextID()
		new.CreatedAt = *now()
		new.CreatedBy = a.GetIdentityFromContext(ctx).Identity()

		// todo
		new.Group = 0

		if err = store.CreateApigwRoute(ctx, svc.store, new); err != nil {
			return err
		}

		q = new

		// send the signal to reload all routes
		if new.Enabled {
			if err = apigw.Service().Reload(ctx); err != nil {
				return err
			}
		}

		return nil
	}()

	return q, svc.recordAction(ctx, qProps, ApigwRouteActionCreate, err)
}

func (svc *apigwRoute) Update(ctx context.Context, upd *types.ApigwRoute) (q *types.ApigwRoute, err error) {
	var (
		qProps = &apigwRouteActionProps{update: upd}
		qq     *types.ApigwRoute
		e      error
	)

	err = func() (err error) {
		if qq, e = store.LookupApigwRouteByID(ctx, svc.store, upd.ID); e != nil {
			return ApigwRouteErrNotFound(qProps)
		}

		if !svc.ac.CanUpdateApigwRoute(ctx, qq) {
			return ApigwRouteErrNotAllowedToUpdate(qProps)
		}

		// temp todo - update itself with the same endpoint
		// if qq, e = store.LookupApigwRouteByEndpoint(ctx, svc.store, upd.Endpoint); e == nil && qq == nil {
		// 	return ApigwRouteErrExistsEndpoint(qProps)
		// }

		upd.UpdatedAt = now()
		upd.CreatedAt = qq.CreatedAt
		upd.UpdatedBy = a.GetIdentityFromContext(ctx).Identity()

		if err = store.UpdateApigwRoute(ctx, svc.store, upd); err != nil {
			return
		}

		q = upd

		// send the signal to reload all route
		if qq.Enabled != upd.Enabled || qq.Enabled && upd.Enabled {
			if err = apigw.Service().Reload(ctx); err != nil {
				return err
			}
		}

		return nil
	}()

	return q, svc.recordAction(ctx, qProps, ApigwRouteActionUpdate, err)
}

func (svc *apigwRoute) DeleteByID(ctx context.Context, ID uint64) (err error) {
	var (
		qProps = &apigwRouteActionProps{}
		q      *types.ApigwRoute
	)

	err = func() (err error) {
		if ID == 0 {
			return ApigwRouteErrInvalidID()
		}

		if q, err = store.LookupApigwRouteByID(ctx, svc.store, ID); err != nil {
			return
		}

		if !svc.ac.CanDeleteApigwRoute(ctx, q) {
			return ApigwRouteErrNotAllowedToDelete(qProps)
		}

		qProps.setRoute(q)

		q.DeletedAt = now()
		q.DeletedBy = a.GetIdentityFromContext(ctx).Identity()

		if err = store.UpdateApigwRoute(ctx, svc.store, q); err != nil {
			return
		}

		// send the signal to reload all queues
		if q.Enabled {
			if err = apigw.Service().Reload(ctx); err != nil {
				return err
			}
		}

		return nil
	}()

	return svc.recordAction(ctx, qProps, ApigwRouteActionDelete, err)
}

func (svc *apigwRoute) UndeleteByID(ctx context.Context, ID uint64) (err error) {
	var (
		qProps = &apigwRouteActionProps{}
		q      *types.ApigwRoute
	)

	err = func() (err error) {
		if ID == 0 {
			return ApigwRouteErrInvalidID()
		}

		if q, err = store.LookupApigwRouteByID(ctx, svc.store, ID); err != nil {
			return
		}

		if !svc.ac.CanDeleteApigwRoute(ctx, q) {
			return ApigwRouteErrNotAllowedToUndelete(qProps)
		}

		qProps.setRoute(q)

		q.DeletedAt = nil
		q.UpdatedBy = a.GetIdentityFromContext(ctx).Identity()

		if err = store.UpdateApigwRoute(ctx, svc.store, q); err != nil {
			return
		}

		// send the signal to reload all queues
		if q.Enabled {
			if err = apigw.Service().Reload(ctx); err != nil {
				return err
			}
		}

		return nil
	}()

	return svc.recordAction(ctx, qProps, ApigwRouteActionDelete, err)
}

func (svc *apigwRoute) Search(ctx context.Context, filter types.ApigwRouteFilter) (r types.ApigwRouteSet, f types.ApigwRouteFilter, err error) {
	var (
		aProps = &apigwRouteActionProps{search: &filter}
	)

	// For each fetched item, store backend will check if it is valid or not
	filter.Check = func(res *types.ApigwRoute) (bool, error) {
		if !svc.ac.CanReadApigwRoute(ctx, res) {
			return false, nil
		}

		return true, nil
	}

	err = func() error {
		if !svc.ac.CanSearchApigwRoutes(ctx) {
			return ApigwRouteErrNotAllowedToSearch()
		}

		if r, f, err = store.SearchApigwRoutes(ctx, svc.store, filter); err != nil {
			return err
		}

		return nil
	}()

	return r, f, svc.recordAction(ctx, aProps, ApigwRouteActionSearch, err)
}

func (svc *apigwRoute) Hits(ctx context.Context, filter types.ApigwProfilerFilter) (r types.ApigwProfilerAggregationSet, f types.ApigwProfilerFilter, err error) {

	f = filter
	// get a list of hits from profiler from apigw service

	// types:
	//  + list of hits, aggregated by endpoint (ie /parse/js)
	//  - list of hits for a specific endpoint
	//  - list of hits for a specific registered route
	r = make([]*types.ApigwProfilerAggregation, 0)

	// how to page
	//  - afterTimestamp
	//  - defaultpagesize = 10

	uDec, _ := base64.URLEncoding.DecodeString(filter.Path)
	filter.Path = string(uDec)

	var (
		list = apigw.Service().Profiler().Dump(profiler.Sort{
			Path:   filter.Path,
			Before: filter.Before,
		})

		tsum, tmin, tmax time.Duration
		ssum, smin, smax int64
		i                uint64 = 1
	)

	for p, v := range list {
		tmin, tmax, tsum = time.Hour, 0, 0
		smin, smax, ssum = math.MaxInt64, 0, 0

		i = 0

		for _, vv := range v {
			var (
				d = *vv.D
				s = vv.R.ContentLength
			)

			if d < tmin {
				tmin = d
			}

			if d > tmax {
				tmax = d
			}

			if s < smin {
				smin = s
			}

			if s > smax {
				smax = s
			}

			tsum += d
			ssum += s
			i++
		}

		r = append(r, &types.ApigwProfilerAggregation{
			Path:  p,
			Count: i,
			Tmin:  tmin.String(),
			Tmax:  tmax.String(),
			Tavg:  (time.Duration(int64(tsum.Seconds()/float64(i))) * time.Second).String(),
			Smin:  smin,
			Smax:  smax,
			Savg:  float64(ssum) / float64(i),
		})

	}

	// r = make(types.ApigwProfilerHitSet, 0)
	// // cp := ""
	// // var td, tmind, tmaxd, tavgd time.Duration
	// var hh = &types.ApigwProfilerHit{}
	// var i = 1

	// for p, v := range list {
	// 	// td = 1
	// 	// if p != cp && cp != "" {

	// 	// }

	// 	hh.Request = *v.R
	// 	hh.D = td.String()

	// 	r = append(r, hh)
	// 	i++
	// }

	return
}
