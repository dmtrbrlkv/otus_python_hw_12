package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"memcache/appsinstalled"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const NORMAL_ERR_RATE = 0.1
const MAX_LINES = 0
const LOG_EVERY = 100000
const MAX_RETRY = 5
const CHAN_SIZE = 1000000

type Result struct {
	processed uint64
	errors    uint64
}

type AppsInstalled struct {
	dev_id   string
	dev_type string
	lat      float64
	lon      float64
	apps     []uint32
}

type Task struct {
	key     string
	message []byte
}

type Config struct {
	pattern string
	idfa    string
	gaid    string
	adid    string
	dvid    string
}

func parseArgs() Config {
	pattern := flag.String("p", "*.tsv.gz", "log file pattern")
	idfa := flag.String("idfa", "127.0.0.1:33013", "idfa memcache address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "gaid memcache address")
	adid := flag.String("adid", "127.0.0.1:33015", "adid memcache address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "dvid memcache address")

	flag.Parse()

	return Config{
		pattern: *pattern,
		idfa:    *idfa,
		gaid:    *gaid,
		adid:    *adid,
		dvid:    *dvid,
	}
}

func dotRename(pth string) {
	head, fn := path.Split(pth)
	new_pth := path.Join(head, "."+fn)
	os.Rename(pth, new_pth)
}

func NewAppsInstalled(dev_id string, dev_type string, lat float64, lon float64, apps []uint32) AppsInstalled {
	a := AppsInstalled{
		dev_id:   dev_id,
		dev_type: dev_type,
		lat:      lat,
		lon:      lon,
		apps:     apps,
	}
	return a
}

func loadToMemcache(addr string, c chan Task, load_result chan Result) {
	var err error

	client := memcache.New(addr)
	//client.Timeout = 30
	var t Task

	var n uint64
	var errors uint64
	for t = range c {

		n++
		if (LOG_EVERY != 0) && (n%LOG_EVERY == 0) {
			log.Printf("%d messages load to %s", n, addr)
		}

		item := memcache.Item{
			Key:   t.key,
			Value: t.message,
		}

		for i := 0; i <= MAX_RETRY; i++ {
			err = client.Set(&item)
			if err == nil {
				break
			}
			log.Printf("Not loaded to %s (%e), retry", addr, err)
			time.Sleep(time.Second * 5)
		}
		if err != nil {
			log.Printf("Not loaded to %s (%e), skip", addr, err)
			errors++
		}

	}

	log.Printf("End load to %s, loaded %d messages", addr, n)
	load_result <- Result{
		processed: n,
		errors:    errors,
	}
}

func serializeAppsinstalledToTask(a AppsInstalled) Task {
	ua := &appsinstalled.UserApps{
		Apps: a.apps,
		Lat:  &a.lat,
		Lon:  &a.lon,
	}

	key := a.dev_type + ":" + a.dev_id

	msg, err := proto.Marshal(ua)
	if err != nil {
		log.Fatal("Failed to encode user apps:", err)
	}

	t := Task{
		key:     key,
		message: msg,
	}

	return t
}

func parseAppsinstalled(line string) AppsInstalled {
	var a AppsInstalled
	var ui64 uint64

	line_parts := strings.Split(line, "\t")
	if len(line_parts) < 5 {
		log.Print("Wrong line format")
		return a
	}

	dev_type := line_parts[0]
	dev_id := line_parts[1]
	lat := line_parts[2]
	lon := line_parts[3]
	raw_apps := line_parts[4]

	if dev_type == "" {
		log.Print("Empty device type")
		return a
	}

	if dev_id == "" {
		log.Print("Empty device id")
		return a
	}

	lat_f, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		log.Print("Not numeric lat")
		return a
	}
	lon_f, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		log.Print("Not numeric lon")
		return a
	}

	apps := strings.Split(raw_apps, ",")
	apps_i := make([]uint32, len(apps))
	for i, app := range apps {
		ui64, err = strconv.ParseUint(app, 10, 64)
		if err != nil {
			log.Print("Not numeric app")
			break
		}

		apps_i[i] = uint32(ui64)
	}
	if err != nil {
		return a
	}

	a = NewAppsInstalled(dev_id, dev_type, lat_f, lon_f, apps_i)
	return a
}

func serializeFileData(fn string, c_map map[string]chan Task, res_chan chan Result) {
	log.Printf("Begin read file %s\n", fn)

	var a AppsInstalled
	var i uint64
	var errors uint64

	gzFile, _ := os.Open(fn)
	defer gzFile.Close()

	gzip_reader, _ := gzip.NewReader(gzFile)

	scanner := bufio.NewScanner(gzip_reader)
	for scanner.Scan() {
		i++

		if (MAX_LINES != 0) && (i > MAX_LINES) {
			break
		}

		if (LOG_EVERY != 0) && (i%LOG_EVERY == 0) {
			log.Printf("%d lines processd %s", i, fn)
		}

		line := scanner.Text()

		a = parseAppsinstalled(line)
		if a.dev_id == "" {
			errors++
			continue
		}

		t := serializeAppsinstalledToTask(a)
		t = t

		c := c_map[a.dev_type]
		c <- t

	}

	log.Printf("End read file %s\n", fn)

	res_chan <- Result{i, errors}
}

func main() {
	start := time.Now()

	config := parseArgs()

	pattern := config.pattern

	device_memc := map[string]string{
		"idfa": config.idfa,
		"gaid": config.gaid,
		"adid": config.adid,
		"dvid": config.dvid,
	}

	load_res_chan := make(chan Result)
	load_proc := 0

	c_map := make(map[string]chan Task)
	for dev_type, addr := range device_memc {
		if CHAN_SIZE > 0 {
			c_map[dev_type] = make(chan Task, CHAN_SIZE)
		} else {
			c_map[dev_type] = make(chan Task)
		}
		go loadToMemcache(addr, c_map[dev_type], load_res_chan)
		load_proc++
	}

	parse_res_chan := make(chan Result)
	parse_proc := 0

	fns, _ := filepath.Glob(pattern)
	for _, fn := range fns {
		go serializeFileData(fn, c_map, parse_res_chan)
		parse_proc++
	}

	var procesed uint64
	var errors uint64

	for i := 1; i <= parse_proc; i++ {
		res := <-parse_res_chan
		//procesed += res.processed
		errors += res.errors
	}

	for _, c := range c_map {
		close(c)
	}

	for i := 1; i <= load_proc; i++ {
		res := <-load_res_chan
		procesed += res.processed
		errors += res.errors
	}

	log.Printf("Processed %d, errors %d", procesed, errors)
	if procesed == 0 {
		log.Print("Not processed")
		return
	}

	err_rate := float64(errors) / float64(procesed)
	if err_rate < NORMAL_ERR_RATE {
		for _, fn := range fns {
			dotRename(fn)
		}
		log.Printf("Acceptable error rate (%f). Successfull load", err_rate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", err_rate, NORMAL_ERR_RATE)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Work time %s", elapsed)

}
