package photon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"time"

	"github.com/influxdata/telegraf"
)

var (
	// NeedMoreSpace ...
	NeedMoreSpace = "need more space"
	// InvalidName ..
	InvalidName = "invalid name"
	// NoFields ...
	NoFields = "no serializable fields"
)

// MetricError is an error causing an entire metric to be unserializable.
type MetricError struct {
	series string
	reason string
}

func (e MetricError) Error() string {
	if e.series != "" {
		return fmt.Sprintf("%q: %s", e.series, e.reason)
	}
	return e.reason
}

// FieldError is an error causing a field to be unserializable.
type FieldError struct {
	reason string
}

func (e FieldError) Error() string {
	return e.reason
}

// Serializer is a serializer for line protocol.
type Serializer struct {
	SenderID string
	buf      bytes.Buffer
}

// NewSerializer create new photon binary serializer
func NewSerializer() *Serializer {
	log.Printf("I! [serializers.photon_bin] NewSerializer is called")
	serializer := &Serializer{}
	serializer.SenderID = "TestSernderId"
	return serializer
}

// Serialize writes the telegraf.Metric to a byte slice.  May produce multiple
// lines of output if longer than maximum line length.  Lines are terminated
// with a newline (LF) char.
func (s *Serializer) Serialize(m telegraf.Metric) ([]byte, error) {
	s.buf.Reset()

	log.Printf("I! [serializers.photon_bin] Serialize is called")

	writeBatchHeader(&s.buf, 1, s.SenderID)

	err := writeMetric(&s.buf, m)
	if err != nil {
		return nil, err
	}

	out := make([]byte, s.buf.Len())
	copy(out, s.buf.Bytes())
	return out, nil
}

// SerializeBatch writes the slice of metrics and returns a byte slice of the
// results.  The returned byte slice may contain multiple lines of data.
func (s *Serializer) SerializeBatch(metrics []telegraf.Metric) ([]byte, error) {
	s.buf.Reset()

	log.Printf("I! [serializers.photon_bin] SerializeBatch is called")

	var (
		writtenMetricsCount int32
		metricBuffer        bytes.Buffer
	)

	for _, m := range metrics {

		err := writeMetric(&metricBuffer, m)
		if err != nil {

			log.Printf("W! [serializers.photon_bin] SerializeBatch got error from writeMetric: %v", err)

			metricBuffer.Reset()
			continue
		}
		metricBuffer.WriteTo(&s.buf)

		writtenMetricsCount++
	}

	var result bytes.Buffer
	writeBatchHeader(&result, writtenMetricsCount, s.SenderID)

	s.buf.WriteTo(&result)
	return result.Bytes(), nil
}

func writeMetric(w *bytes.Buffer, m telegraf.Metric) error {
	var (
		err error
	)

	writeString(w, m.Name())
	writeInt16(w, 1)

	writeTime(w, m.Time())

	switch len(m.FieldList()) {
	case 0:
		log.Printf(
			"W! [serializers.photon_bin] could not serialize metric %v; It has no fields. discarding it", m.Name())
		return newMetricError(NoFields)
	case 1:
		flds := m.FieldList()
		err = appendFieldValue(w, m.Name(), flds[0].Key, flds[0].Value)
		if err != nil {
			return newMetricError(NoFields)
		}
	default:
		log.Printf("D! [serializers.photon_bin] metric %v; has MANY! fields", m.Name())
		for _, k := range m.FieldList() {
			log.Printf("D! [serializers.photon_bin] metric %v; has field: %v", m.Name(), k)

			ok, valueToWrite := isValidFieldTypeAndValue(k.Value)
			if !ok {
				continue
			}
			if k.Key == "value_mean" || k.Key == "value" {
				appendFloatField(w, valueToWrite)
				return nil
			}
		}
		err = newMetricError(NoFields)
	}

	return err
}

func writeBatchHeader(w *bytes.Buffer, len int32, senderId string) error {
	w.WriteByte(0xee)
	w.WriteByte(0xff)

	writeTime(w, time.Now())
	writeInt32(w, int32(len))
	err := writeString(w, senderId)

	return err
}

func write7BitEncodedInt(w io.ByteWriter, value int32) {
	// Write out an int 7 bits at a time.  The high bit of the byte,
	// when on, tells reader to continue reading more bytes.
	v := uint32(value) // support negative numbers
	for v >= 0x80 {
		w.WriteByte(byte(v | 0x80))
		v >>= 7
	}
	w.WriteByte(byte(v))
}

func writeString(w *bytes.Buffer, str string) error {

	l := len(str)

	write7BitEncodedInt(w, int32(l))
	_, err := w.WriteString(str)
	return err
}

func writeTime(w io.ByteWriter, t time.Time) {

	d := t.Unix()*10000000 + 621355968000000000

	writeInt64Value(w, d)
}

func writeInt32(w io.ByteWriter, value int32) error {

	err := w.WriteByte(byte(value))
	err = w.WriteByte(byte(value >> 8))
	err = w.WriteByte(byte(value >> 16))
	err = w.WriteByte(byte(value >> 24))

	return err
}

func writeInt16(w io.ByteWriter, value int16) error {

	err := w.WriteByte(byte(value))
	err = w.WriteByte(byte(value >> 8))
	return err
}

func newMetricError(reason string) *MetricError {
	return &MetricError{reason: reason}
}

func isFloat32Valid(v float32) error {
	if v != v {
		return &FieldError{"is NaN"}
	}

	if math.MaxFloat32 < v {
		return &FieldError{"is Inf"}
	}
	return nil
}

func isValidFieldTypeAndValue(value interface{}) (bool, float32) {
	var valueToWrite float32
	switch v := value.(type) {
	case int32:
		valueToWrite = float32(v)
	case uint32:
		valueToWrite = float32(v)
	case int64:
		valueToWrite = float32(v)
	case uint64:
		valueToWrite = float32(v)
	case float32:
		valueToWrite = v
	case float64:
		if math.IsNaN(v) {
			return false, 0.0
		}

		if math.IsInf(v, 0) {
			return false, 0.0
		}

		valueToWrite = float32(v)
	default:
		return false, 0.0
	}
	err := isFloat32Valid(valueToWrite)
	if err != nil {
		return false, 0.0
	}

	return true, valueToWrite
}

func appendFieldValue(w io.Writer, metricName, fieldName string, value interface{}) error {

	if value == nil {
		return &FieldError{fmt.Sprintf("metric %v does not have field %v", metricName, fieldName)}
	}

	var valueToWrite float32
	switch v := value.(type) {
	case int32:
		valueToWrite = float32(v)
	case uint32:
		valueToWrite = float32(v)
	case int64:
		valueToWrite = float32(v)
	case uint64:
		valueToWrite = float32(v)
	case float32:
		valueToWrite = v
	case float64:
		if math.IsNaN(v) {
			return &FieldError{"is NaN"}
		}

		if math.IsInf(v, 0) {
			return &FieldError{"is Inf"}
		}

		valueToWrite = float32(v)
	default:
		log.Printf("D! [serializers.photon_bin] invalid value type: %T", v)
		return &FieldError{fmt.Sprintf("invalid value type: %T", v)}
	}

	err := isFloat32Valid(valueToWrite)
	if err != nil {
		return err
	}

	appendFloatField(w, valueToWrite)
	return nil
}

func writeInt64Value(w io.ByteWriter, value int64) {
	w.WriteByte(byte(value))
	w.WriteByte(byte(value >> 8))
	w.WriteByte(byte(value >> 16))
	w.WriteByte(byte(value >> 24))
	w.WriteByte(byte(value >> 32))
	w.WriteByte(byte(value >> 40))
	w.WriteByte(byte(value >> 48))
	w.WriteByte(byte(value >> 56))
}

func appendFloatField(w io.Writer, value float32) {
	binary.Write(w, binary.LittleEndian, value)
}
