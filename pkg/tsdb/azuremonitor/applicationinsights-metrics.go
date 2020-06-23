package azuremonitor

import "encoding/json"

// MetricsResult a metric result.
type MetricsResult struct {
	Value *MetricsResultInfo `json:"value,omitempty"`
}

// MetricsResultInfo a metric result data.
type MetricsResultInfo struct {
	// AdditionalProperties - Unmatched properties from the message are deserialized this collection
	AdditionalProperties map[string]interface{} `json:""`
	// Start - Start time of the metric.
	Start string `json:"start,omitempty"`
	// End - Start time of the metric.
	End string `json:"end,omitempty"`
	// Interval - The interval used to segment the metric data.
	Interval *string `json:"interval,omitempty"`
	// Segments - Segmented metric data (if segmented).
	Segments *[]MetricsSegmentInfo `json:"segments,omitempty"`
}

// MetricsSegmentInfo a metric segment
type MetricsSegmentInfo struct {
	// AdditionalProperties - Unmatched properties from the message are deserialized this collection
	AdditionalProperties map[string]interface{} `json:""`
	// Start - Start time of the metric segment (only when an interval was specified).
	Start string `json:"start,omitempty"`
	// End - Start time of the metric segment (only when an interval was specified).
	End string `json:"end,omitempty"`
	// Segments - Segmented metric data (if further segmented).
	Segments *[]MetricsSegmentInfo `json:"segments,omitempty"`
}

// UnmarshalJSON is the custom unmarshaler for MetricsResultInfo struct.
func (mri *MetricsSegmentInfo) UnmarshalJSON(body []byte) error {
	var m map[string]*json.RawMessage
	err := json.Unmarshal(body, &m)
	if err != nil {
		return err
	}
	for k, v := range m {
		switch k {
		default:
			if v != nil {
				var additionalProperties interface{}
				err = json.Unmarshal(*v, &additionalProperties)
				if err != nil {
					return err
				}
				if mri.AdditionalProperties == nil {
					mri.AdditionalProperties = make(map[string]interface{})
				}
				mri.AdditionalProperties[k] = additionalProperties
			}
		case "start":
			if v != nil {
				var start string
				err = json.Unmarshal(*v, &start)
				if err != nil {
					return err
				}
				mri.Start = start
			}
		case "end":
			if v != nil {
				var end string
				err = json.Unmarshal(*v, &end)
				if err != nil {
					return err
				}
				mri.End = end
			}
		case "segments":
			if v != nil {
				var segments []MetricsSegmentInfo
				err = json.Unmarshal(*v, &segments)
				if err != nil {
					return err
				}
				mri.Segments = &segments
			}
		}
	}

	return nil
}

// UnmarshalJSON is the custom unmarshaler for MetricsResultInfo struct.
func (mri *MetricsResultInfo) UnmarshalJSON(body []byte) error {
	var m map[string]*json.RawMessage
	err := json.Unmarshal(body, &m)
	if err != nil {
		return err
	}
	for k, v := range m {
		switch k {
		default:
			if v != nil {
				var additionalProperties interface{}
				err = json.Unmarshal(*v, &additionalProperties)
				if err != nil {
					return err
				}
				if mri.AdditionalProperties == nil {
					mri.AdditionalProperties = make(map[string]interface{})
				}
				mri.AdditionalProperties[k] = additionalProperties
			}
		case "start":
			if v != nil {
				var start string
				err = json.Unmarshal(*v, &start)
				if err != nil {
					return err
				}
				mri.Start = start
			}
		case "end":
			if v != nil {
				var end string
				err = json.Unmarshal(*v, &end)
				if err != nil {
					return err
				}
				mri.End = end
			}
		case "interval":
			if v != nil {
				var interval string
				err = json.Unmarshal(*v, &interval)
				if err != nil {
					return err
				}
				mri.Interval = &interval
			}
		case "segments":
			if v != nil {
				var segments []MetricsSegmentInfo
				err = json.Unmarshal(*v, &segments)
				if err != nil {
					return err
				}
				mri.Segments = &segments
			}
		}
	}

	return nil
}
