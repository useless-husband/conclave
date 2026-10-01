package sim

// tracer keeps the most recent trace lines (or all of them).
type tracer struct {
	max   int
	buf   []traceLine
	start int
	total int
}

type traceLine struct {
	at   Time
	text string
}

func newTracer(max int) *tracer { return &tracer{max: max} }

func (t *tracer) add(at Time, text string) {
	t.total++
	if t.max < 0 || len(t.buf) < t.max {
		t.buf = append(t.buf, traceLine{at, text})
		return
	}
	t.buf[t.start] = traceLine{at, text}
	t.start = (t.start + 1) % len(t.buf)
}

func (t *tracer) lines() []string {
	out := make([]string, 0, len(t.buf)+1)
	if dropped := t.total - len(t.buf); dropped > 0 {
		out = append(out, "... "+itoa(uint64(dropped))+" earlier lines omitted")
	}
	for i := range t.buf {
		l := t.buf[(t.start+i)%len(t.buf)]
		out = append(out, l.at.String()+"  "+l.text)
	}
	return out
}
