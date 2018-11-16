package net

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/system"
)

type interfaceMetrics struct {
	time          time.Time
	bytesReceived uint64
	bytesSent     uint64
}

type NetIOStats struct {
	filter filter.Filter
	ps     system.PS

	skipChecks          bool
	IgnoreProtocolStats bool
	Interfaces          []string
	prevValues          map[string]interfaceMetrics
}

func (_ *NetIOStats) Description() string {
	return "Read metrics about network interface usage"
}

var netSampleConfig = `
  ## By default, telegraf gathers stats from any up interface (excluding loopback)
  ## Setting interfaces will tell it to gather these explicit interfaces,
  ## regardless of status.
  ##
  # interfaces = ["eth0"]
  ##
  ## On linux systems telegraf also collects protocol stats.
  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.
  ##
  # ignore_protocol_stats = false
  ##
`

func (_ *NetIOStats) SampleConfig() string {
	return netSampleConfig
}

func (s *NetIOStats) Gather(acc telegraf.Accumulator) error {
	netio, err := s.ps.NetIO()
	if err != nil {
		return fmt.Errorf("error getting net io info: %s", err)
	}

	if s.filter == nil {
		if s.filter, err = filter.Compile(s.Interfaces); err != nil {
			return fmt.Errorf("error compiling filter: %s", err)
		}
	}

	var (
		bytesSent, bytesRecv, packetsSent, packetsRecv uint64 = 0, 0, 0, 0
		errIn, errOut, dropIn, dropOut                 uint64 = 0, 0, 0, 0
		totalBytesInPerSec, totalBytesOutPerSec        uint64 = 0, 0
	)

	now := time.Now()

	for _, io := range netio {
		if len(s.Interfaces) != 0 {
			var found bool

			if s.filter.Match(io.Name) {
				found = true
			}

			if !found {
				continue
			}
		} else if !s.skipChecks {
			iface, err := net.InterfaceByName(io.Name)
			if err != nil {
				continue
			}

			if iface.Flags&net.FlagLoopback == net.FlagLoopback {
				continue
			}

			if iface.Flags&net.FlagUp == 0 {
				continue
			}
		}

		tags := map[string]string{
			"interface": io.Name,
		}

		var (
			bytesInPerSec, bytesOutPerSec uint64 = 0, 0
		)

		prevValues, ok := s.prevValues[io.Name]
		if ok {
			dur := now.Sub(prevValues.time)
			bytesInPerSec = uint64(float64(io.BytesRecv-prevValues.bytesReceived) / dur.Seconds())
			bytesOutPerSec = uint64(float64(io.BytesSent-prevValues.bytesSent) / dur.Seconds())
		}

		fields := map[string]interface{}{
			"bytes_sent":        io.BytesSent,
			"bytes_recv":        io.BytesRecv,
			"packets_sent":      io.PacketsSent,
			"packets_recv":      io.PacketsRecv,
			"err_in":            io.Errin,
			"err_out":           io.Errout,
			"drop_in":           io.Dropin,
			"drop_out":          io.Dropout,
			"bytes_in_per_sec":  bytesInPerSec,
			"bytes_out_per_sec": bytesOutPerSec,
		}

		//log.Printf("D! [input.net] intrf: %v bytes_recv: %v", io.Name, io.BytesRecv)

		bytesSent += io.BytesSent
		bytesRecv += io.BytesRecv
		packetsSent += io.PacketsSent
		packetsRecv += io.PacketsRecv
		errIn += io.Errin
		errOut += io.Errout
		dropIn += io.Dropin
		dropOut += io.Dropout
		totalBytesInPerSec += bytesInPerSec
		totalBytesOutPerSec += bytesOutPerSec

		acc.AddCounter("net", fields, tags)

		s.prevValues[io.Name] = interfaceMetrics{
			time:          now,
			bytesReceived: io.BytesRecv,
			bytesSent:     io.BytesSent,
		}
	}

	tags := map[string]string{
		"interface": "all",
	}

	fields := map[string]interface{}{
		"total_bytes_sent":           bytesSent,
		"total_bytes_recv":           bytesRecv,
		"total_packets_sent":         packetsSent,
		"total_packets_recv":         packetsRecv,
		"total_err_in":               errIn,
		"total_err_out":              errOut,
		"total_drop_in":              dropIn,
		"total_drop_out":             dropOut,
		"total_bytes_in_per_sec":     totalBytesInPerSec,
		"total_bytes_out_per_sec":    totalBytesOutPerSec,
		"total_bytes_in_out_per_sec": totalBytesInPerSec + totalBytesOutPerSec,
	}

	acc.AddCounter("net", fields, tags)

	// Get system wide stats for different network protocols
	// (ignore these stats if the call fails)
	if !s.IgnoreProtocolStats {
		netprotos, _ := s.ps.NetProto()
		fields := make(map[string]interface{})
		for _, proto := range netprotos {
			for stat, value := range proto.Stats {
				name := fmt.Sprintf("%s_%s", strings.ToLower(proto.Protocol),
					strings.ToLower(stat))
				fields[name] = value
			}
		}
		tags := map[string]string{
			"interface": "all",
		}
		acc.AddFields("net", fields, tags)
	}

	return nil
}

func init() {
	inputs.Add("net", func() telegraf.Input {
		return &NetIOStats{ps: system.NewSystemPS(), prevValues: make(map[string]interfaceMetrics)}
	})
}
