package request

// This file is auto-generated.
//
// Changes to this file may cause incorrect behavior and will be lost if
// the code is regenerated.
//
// Definitions file that controls how this file is generated:
//

import (
	"encoding/json"
	"fmt"
	"github.com/cortezaproject/corteza-server/pkg/payload"
	"github.com/go-chi/chi/v5"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// dummy vars to prevent
// unused imports complain
var (
	_ = chi.URLParam
	_ = multipart.ErrMessageTooLarge
	_ = payload.ParseUint64s
	_ = strings.ToLower
	_ = io.EOF
	_ = fmt.Errorf
	_ = json.NewEncoder
)

type (
	// Internal API interface
	ApigwProfilerList struct {
		// Path GET parameter
		//
		// Filter by request path
		Path string

		// Before GET parameter
		//
		// Filter by request date
		Before *time.Time

		// Sort GET parameter
		//
		// Sort items
		Sort string

		// Limit GET parameter
		//
		// Limit
		Limit uint
	}
)

// NewApigwProfilerList request
func NewApigwProfilerList() *ApigwProfilerList {
	return &ApigwProfilerList{}
}

// Auditable returns all auditable/loggable parameters
func (r ApigwProfilerList) Auditable() map[string]interface{} {
	return map[string]interface{}{
		"path":   r.Path,
		"before": r.Before,
		"sort":   r.Sort,
		"limit":  r.Limit,
	}
}

// Auditable returns all auditable/loggable parameters
func (r ApigwProfilerList) GetPath() string {
	return r.Path
}

// Auditable returns all auditable/loggable parameters
func (r ApigwProfilerList) GetBefore() *time.Time {
	return r.Before
}

// Auditable returns all auditable/loggable parameters
func (r ApigwProfilerList) GetSort() string {
	return r.Sort
}

// Auditable returns all auditable/loggable parameters
func (r ApigwProfilerList) GetLimit() uint {
	return r.Limit
}

// Fill processes request and fills internal variables
func (r *ApigwProfilerList) Fill(req *http.Request) (err error) {

	{
		// GET params
		tmp := req.URL.Query()

		if val, ok := tmp["path"]; ok && len(val) > 0 {
			r.Path, err = val[0], nil
			if err != nil {
				return err
			}
		}
		if val, ok := tmp["before"]; ok && len(val) > 0 {
			r.Before, err = payload.ParseISODatePtrWithErr(val[0])
			if err != nil {
				return err
			}
		}
		if val, ok := tmp["sort"]; ok && len(val) > 0 {
			r.Sort, err = val[0], nil
			if err != nil {
				return err
			}
		}
		if val, ok := tmp["limit"]; ok && len(val) > 0 {
			r.Limit, err = payload.ParseUint(val[0]), nil
			if err != nil {
				return err
			}
		}
	}

	return err
}
