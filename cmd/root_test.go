package cmd

import (
	"testing"
)

func TestDispatch(t *testing.T) {
	rows := &implNextScanner{
		counter: 30000,
	}
	jobs := make(chan []*Doc, 100)
	jobsize := 100
	go func() {
		for range jobs {
		}
	}()
	dispatch(rows, "esindex", "estable", jobs, jobsize)
}

type implNextScanner struct {
	counter int
}

func (i *implNextScanner) Next() bool {
	i.counter--
	return i.counter == 0
}

func (i *implNextScanner) Scan(dist ...interface{}) error {
	for i := range dist {
		dist[i] = "hihi"
	}
	return nil
}
