package types

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/cortezaproject/corteza-server/pkg/filter"
	h "github.com/cortezaproject/corteza-server/pkg/http"
	"github.com/pkg/errors"
)

type (
	ApigwRoute struct {
		ID       uint64         `json:"routeID,string"`
		Endpoint string         `json:"endpoint"`
		Method   string         `json:"method"`
		Enabled  bool           `json:"enabled"`
		Group    uint64         `json:"group,string"`
		Meta     ApigwRouteMeta `json:"meta"`

		CreatedAt time.Time  `json:"createdAt,omitempty"`
		CreatedBy uint64     `json:"createdBy,string" `
		UpdatedAt *time.Time `json:"updatedAt,omitempty"`
		UpdatedBy uint64     `json:"updatedBy,string,omitempty" `
		DeletedAt *time.Time `json:"deletedAt,omitempty"`
		DeletedBy uint64     `json:"deletedBy,string,omitempty" `
	}

	ApigwProfilerHit struct {
		Request h.Request  `json:"request"`
		Ts      *time.Time `json:"time_start"`
		Tf      *time.Time `json:"time_finish"`
		D       string     `json:"time_duration"`
	}

	ApigwProfilerAggregation struct {
		Path  string  `json:"path"`
		Count uint64  `json:"count"`
		Smin  int64   `json:"size_min"`
		Smax  int64   `json:"size_max"`
		Savg  float64 `json:"size_avg"`
		Tmin  string  `json:"time_min"`
		Tmax  string  `json:"time_max"`
		Tavg  string  `json:"time_avg"`
	}

	ApigwRouteMeta struct {
		Debug bool `json:"debug"`
		Async bool `json:"async"`
	}

	ApigwRouteFilter struct {
		Route string `json:"route"`
		Group string `json:"group"`

		Deleted  filter.State `json:"deleted"`
		Disabled filter.State `json:"disabled"`

		// Check fn is called by store backend for each resource found function can
		// modify the resource and return false if store should not return it
		//
		// Store then loads additional resources to satisfy the paging parameters
		Check func(*ApigwRoute) (bool, error) `json:"-"`

		filter.Sorting
		filter.Paging
	}

	ApigwProfilerFilter struct {
		Path   string     `json:"path,omitempty"`
		Before *time.Time `json:"before,omitempty"`

		filter.Sorting
	}
)

func (cc *ApigwRouteMeta) Scan(value interface{}) error {
	//lint:ignore S1034 This typecast is intentional, we need to get []byte out of a []uint8
	switch value.(type) {
	case nil:
		*cc = ApigwRouteMeta{}
	case []uint8:
		b := value.([]byte)
		if err := json.Unmarshal(b, cc); err != nil {
			return errors.Wrapf(err, "cannot scan '%v' into ApigwRouteMeta", string(b))
		}
	}

	return nil
}

func (cc ApigwRouteMeta) Value() (driver.Value, error) {
	return json.Marshal(cc)
}
