package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/prashant-shahi/dgraph-bench/tasks"
)

const (
	maxDirectFriends = 1000
	k                = 100
)

var (
	output  = flag.String("output", "out.rdf.gz", "Output .gz file")
	rf      = flag.String("rf", "rf.rdf.gz", "Relationship .gz file")
	maxUid  = flag.Int("maxUid", 10000000, "10mi Nodes default")
	boolPtr = flag.Bool("CSV", false, "generate CSV")
	rels    = []string{"rel_1", "rel_2", "rel_3"}
)

func main() {
	var recordCount int
	flag.Parse()
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	w := gzip.NewWriter(f)
	if *boolPtr {
		f1, err := os.OpenFile(*rf, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			panic(err)
		}
		w1 := gzip.NewWriter(f1)
		writeCSVHeader(w, "xid:ID", "name", "surname", "patronymic", "dt")
		if _, err := w1.Write([]byte(":START_ID,:END_ID,:TYPE\n")); err != nil {
			panic(err)
		}
		for i := 0; i < *maxUid; i++ {
			writeCSV(w,
				fmt.Sprintf("\"%d\"", i),
				fmt.Sprintf("\"%s\"", tasks.RandString(10)),
				fmt.Sprintf("\"%s\"", tasks.RandString(10)),
				fmt.Sprintf("\"%s\"", tasks.RandString(10)),
				fmt.Sprintf("\"%d\"", time.Now().UnixNano()))
			writeSpace(w, fmt.Sprintf("%d", i))
			s1 := fmt.Sprintf("_:uid%d, _:uid%d, rel_1\n", i, rand.Intn(*maxUid))
			s2 := fmt.Sprintf("_:uid%d, _:uid%d, rel_2\n", i, rand.Intn(*maxUid))
			s3 := fmt.Sprintf("_:uid%d, _:uid%d, rel_3\n", i, rand.Intn(*maxUid))
			_, err := w1.Write([]byte(fmt.Sprintf("%s%s%s", s1, s2, s3)))
			if err != nil {
				panic(err)
			}
			recordCount++
			if recordCount%10000 == 0 {
				fmt.Println("Processed: ", recordCount)
			}
		}
		if err := w.Flush(); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		if err := f.Close(); err != nil {
			panic(err)
		}
		if err := w1.Flush(); err != nil {
			panic(err)
		}
		if err := w1.Close(); err != nil {
			panic(err)
		}
		if err := f1.Close(); err != nil {
			panic(err)
		}
	} else {
		for i := 1; i <= *maxUid; i++ {
			meID := fmt.Sprintf("_:uid%d", i)
			writeNQuad(w, meID, "xid", fmt.Sprintf("\"%d\"", i))
			writeNQuad(w, meID, "name", fmt.Sprintf("\"%s\"", tasks.RandString(10)))
			writeNQuad(w, meID, "surname", fmt.Sprintf("\"%s\"", tasks.RandString(10)))
			writeNQuad(w, meID, "patronymic", fmt.Sprintf("\"%s\"", tasks.RandString(10)))
			writeNQuad(w, meID, "dt", fmt.Sprintf("\"%d\"", time.Now().UnixNano()))
			recordCount += 8
			for _, rel := range rels {
				fid := rand.Intn(*maxUid)
				for fid == i {
					fid = rand.Intn(*maxUid)
				}
				writeNQuad(w, meID, rel, fmt.Sprintf("<_:uid%d>", fid))
			}
			if recordCount%10000 == 0 {
				fmt.Println("Processed: ", recordCount)
			}
		}
		if err := w.Flush(); err != nil {
			panic(err)
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		if err := f.Close(); err != nil {
			panic(err)
		}
	}
	fmt.Println("**************************** total RDF records ", recordCount)
}
func randomNum() int {
	// N(t) = N0 * e^(-k*t)
	return 5 + int(maxDirectFriends*math.Exp(-k*rand.Float64()))
}
func writeCSVHeader(w *gzip.Writer, xid, name, surname, patronymic, dt string) {
	str := fmt.Sprintf("%v, %v, %v, %v, %v\n", xid, name, surname, patronymic, dt)
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func write99(w *gzip.Writer, a string) {
	str := fmt.Sprintf("99")
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func writeq(w *gzip.Writer, a string) {
	str := fmt.Sprintf("\"")
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func writeSpace(w *gzip.Writer, a string) {
	str := fmt.Sprintf("\n")
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func writeCSV(w *gzip.Writer, xid, name, surname, patronymic, dt string) {
	str := fmt.Sprintf("%s,%s,%s,%s,%s", xid, name, surname, patronymic, dt)
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func writeCSVFriends(w *gzip.Writer, Friends string) {
	str := fmt.Sprintf("%v", Friends)
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
func writeNQuad(w *gzip.Writer, s, p, o string) {
	str := fmt.Sprintf("<%v> <%v> %v .\n", s, p, o)
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}
