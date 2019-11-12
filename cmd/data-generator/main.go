package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/linuxerwang/dgraph-bench/tasks"
)

const (
	maxDirectFriends = 1000

	k = 100
)

var (
	output  = flag.String("output", "out.rdf.gz", "Output .gz file")
	maxUid  = flag.Int("maxUid", 10000000, "10mi Nodes default")
	boolPtr = flag.Bool("CSV", false, "generate CSV")
)

func main() {
	flag.Parse()
	f, err := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	w := gzip.NewWriter(f)
	if *boolPtr {
		writeCSVHeader(w, "uid", "xid", "name", "age", "created_at", "updated_at", "friend_of")
		for i := 1; i <= *maxUid; i++ {
			meID := fmt.Sprintf("_:uid%d", i)

			writeCSV(w, meID,
				fmt.Sprintf("\"%d\"", i),
				fmt.Sprintf("\"%s\"", tasks.RandString(10)),
				fmt.Sprintf("\"%d\"", 18+rand.Intn(80)),
				fmt.Sprintf("\"%d\"", time.Now().UnixNano()),
				fmt.Sprintf("\"%d\"", time.Now().UnixNano()),
			)
			writeq(w, fmt.Sprintf("%d", i))
			friendCnt := randomNum()
			for j := 1; j <= friendCnt; j++ {
				fID := rand.Intn(*maxUid)
				for fID == i {
					fID = rand.Intn(*maxUid)
				}
				writeCSVFriends(w, fmt.Sprintf("%d,", fID))
			}
			write99(w, fmt.Sprintf("%d", i))
			writeq(w, fmt.Sprintf("%d", i))
			writeSpace(w, fmt.Sprintf("%d", i))
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
	} else {
		for i := 1; i <= *maxUid; i++ {
			meID := fmt.Sprintf("_:m.%d", i)
			writeNQuad(w, meID, "xid", fmt.Sprintf("\"%d\"", i))
			writeNQuad(w, meID, "name", fmt.Sprintf("\"%s\"", tasks.RandString(10)))
			writeNQuad(w, meID, "age", fmt.Sprintf("\"%d\"", 18+rand.Intn(80)))
			writeNQuad(w, meID, "created_at", fmt.Sprintf("\"%d\"", time.Now().UnixNano()))
			writeNQuad(w, meID, "updated_at", fmt.Sprintf("\"%d\"", time.Now().UnixNano()))

			friendCnt := randomNum()
			for j := 1; j <= friendCnt; j++ {
				fID := rand.Intn(*maxUid)
				for fID == i {
					fID = rand.Intn(*maxUid)
				}
				writeNQuad(w, meID, "friend_of", fmt.Sprintf("<_:m.%d>", fID))
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

}

func randomNum() int {
	// N(t) = N0 * e^(-k*t)
	return 5 + int(maxDirectFriends*math.Exp(-k*rand.Float64()))
}

func writeCSVHeader(w *gzip.Writer, uid, xid, name, age, created_at, updated_at, friend_of string) {
	str := fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v \n", uid, xid, name, age, created_at, updated_at, friend_of)
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
	str := fmt.Sprintf("\n ")
	if _, err := w.Write([]byte(str)); err != nil {
		panic(err)
	}
}

func writeCSV(w *gzip.Writer, uid, xid, name, age, created_at, updated_at string) {
	str := fmt.Sprintf("%v, %v, %v, %v, %v, %v, ", uid, xid, name, age, created_at, updated_at)
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
