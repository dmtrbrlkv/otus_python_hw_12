package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
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
const CHAN_SIZE = 3000000
const BUF_SIZE = 0
const TIMEOUT = 30

type Result struct {
	processed uint64
	errors    uint64
	fn        string
	err       error
}

type AppsInstalled struct {
	devId   string
	devType string
	lat     float64
	lon     float64
	apps    []uint32
}

type Task struct {
	key     string
	message []byte
}

type Config struct {
	pattern  string
	idfa     string
	gaid     string
	adid     string
	dvid     string
	chanSize int
	bufSize  int
	timeout  int
}

func (c Config) String() string {
	return fmt.Sprintf("pattern = %s, idfa = %s, gaid = %s, adid = %s, dvid = %s, chan_size = %d, buf_size = %d, timeout = %d",
		c.pattern, c.idfa, c.gaid, c.adid, c.dvid, c.chanSize, c.bufSize, c.timeout)
}

func parseArgs() Config {
	pattern := flag.String("p", "*.tsv.gz", "log file pattern")
	idfa := flag.String("idfa", "127.0.0.1:33013", "idfa memcache address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "gaid memcache address")
	adid := flag.String("adid", "127.0.0.1:33015", "adid memcache address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "dvid memcache address")
	chanSize := flag.Int("c", CHAN_SIZE, "chanel size")
	bufSize := flag.Int("b", BUF_SIZE, "buffer size")
	timeout := flag.Int("t", TIMEOUT, "memcache timeout")

	flag.Parse()

	return Config{
		pattern:  *pattern,
		idfa:     *idfa,
		gaid:     *gaid,
		adid:     *adid,
		dvid:     *dvid,
		chanSize: *chanSize,
		bufSize:  *bufSize,
		timeout:  *timeout,
	}
}

func addToChanelWithCache(taskCache map[string][]Task, cMap map[string]chan []Task, key string, task Task, cacheSize int) {
	tasks, ok := taskCache[key]
	if !ok {
		tasks = make([]Task, 0)
	}

	tasks = append(tasks, task)
	taskCache[key] = tasks

	if cacheSize == 0 || len(tasks) >= cacheSize {
		c := cMap[key]
		c <- tasks
		tasks = make([]Task, 0)
		taskCache[key] = tasks
	}

}

func flushQueueCache(taskCache map[string][]Task, cMap map[string]chan []Task, key string) {
	tasks, ok := taskCache[key]
	if !ok {
		return
	}

	c := cMap[key]
	c <- tasks
	tasks = make([]Task, 0)
	taskCache[key] = tasks
}

func dotRename(pth string) {
	head, fn := path.Split(pth)
	newPth := path.Join(head, "."+fn)
	os.Rename(pth, newPth)
}

func NewAppsInstalled(devId string, devType string, lat float64, lon float64, apps []uint32) AppsInstalled {
	a := AppsInstalled{
		devId:   devId,
		devType: devType,
		lat:     lat,
		lon:     lon,
		apps:    apps,
	}
	return a
}

func loadToMemcache(addr string, c chan []Task, timeout int, loadResult chan Result) {
	var err error

	client := memcache.New(addr)
	client.Timeout = time.Second * time.Duration(timeout)
	var t Task
	var tasks []Task

	var n uint64
	var errors uint64
	for tasks = range c {
		for _, t = range tasks {

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
				log.Printf("Not loaded to %s (%s), retry", addr, err)
				time.Sleep(time.Second * 5)
			}
			if err != nil {
				log.Printf("Not loaded to %s (%s), skip", addr, err)
				errors++
			}
		}
	}

	log.Printf("End load to %s, loaded %d messages", addr, n)
	loadResult <- Result{
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

	key := a.devType + ":" + a.devId

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

	lineParts := strings.Split(line, "\t")
	if len(lineParts) < 5 {
		log.Print("Wrong line format")
		return a
	}

	devType := lineParts[0]
	devId := lineParts[1]
	lat := lineParts[2]
	lon := lineParts[3]
	rawApps := lineParts[4]

	if devType == "" {
		log.Print("Empty device type")
		return a
	}

	if devId == "" {
		log.Print("Empty device id")
		return a
	}

	latF, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		log.Print("Not numeric lat")
		return a
	}
	lonF, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		log.Print("Not numeric lon")
		return a
	}

	apps := strings.Split(rawApps, ",")
	appsI := make([]uint32, len(apps))
	for i, app := range apps {
		ui64, err = strconv.ParseUint(app, 10, 64)
		if err != nil {
			log.Print("Not numeric app")
			break
		}

		appsI[i] = uint32(ui64)
	}
	if err != nil {
		return a
	}

	a = NewAppsInstalled(devId, devType, latF, lonF, appsI)
	return a
}

func serializeFileData(fn string, cMap map[string]chan []Task, resChan chan Result, cacheSize int) {
	taskCache := make(map[string][]Task)

	log.Printf("Begin read file %s\n", fn)

	var a AppsInstalled
	var i uint64
	var errors uint64
	var err error

	gzFile, err := os.Open(fn)
	if err != nil {
		log.Printf("Error open file %s (%s)", fn, err)
		resChan <- Result{i, errors, fn, err}
		return
	}

	defer gzFile.Close()

	gzipReader, err := gzip.NewReader(gzFile)
	if err != nil {
		log.Printf("Error open file %s (%s)", fn, err)
		resChan <- Result{i, errors, fn, err}
		return
	}

	scanner := bufio.NewScanner(gzipReader)
	for scanner.Scan() {
		i++

		if (MAX_LINES != 0) && (i > MAX_LINES) {
			break
		}

		if (LOG_EVERY != 0) && (i%LOG_EVERY == 0) {
			log.Printf("%d lines processed %s", i, fn)
		}

		line := scanner.Text()

		a = parseAppsinstalled(line)
		if a.devId == "" {
			errors++
			continue
		}

		t := serializeAppsinstalledToTask(a)

		addToChanelWithCache(taskCache, cMap, a.devType, t, cacheSize)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error read file %s\n", fn)
		resChan <- Result{i, errors, fn, err}
		return
	}

	for key := range cMap {
		flushQueueCache(taskCache, cMap, key)
	}

	log.Printf("End read file %s\n", fn)

	resChan <- Result{i, errors, fn, err}
}

func main() {
	var processedFiles []string

	start := time.Now()

	config := parseArgs()

	log.Printf("Run with options: %s", config)

	pattern := config.pattern

	deviceMemc := map[string]string{
		"idfa": config.idfa,
		"gaid": config.gaid,
		"adid": config.adid,
		"dvid": config.dvid,
	}

	timeout := config.timeout

	loadResChan := make(chan Result)
	loadProc := 0

	cMap := make(map[string]chan []Task)
	for devType, addr := range deviceMemc {
		if config.chanSize > 0 {
			cMap[devType] = make(chan []Task, CHAN_SIZE)
		} else {
			cMap[devType] = make(chan []Task)
		}
		go loadToMemcache(addr, cMap[devType], timeout, loadResChan)
		loadProc++
	}

	parseResChan := make(chan Result)
	parseProc := 0

	fns, _ := filepath.Glob(pattern)
	for _, fn := range fns {
		go serializeFileData(fn, cMap, parseResChan, config.bufSize)
		parseProc++
	}

	var processed uint64
	var errors uint64

	for i := 1; i <= parseProc; i++ {
		res := <-parseResChan
		errors += res.errors
		if res.err == nil {
			processedFiles = append(processedFiles, res.fn)
		}
	}

	for _, c := range cMap {
		close(c)
	}

	for i := 1; i <= loadProc; i++ {
		res := <-loadResChan
		processed += res.processed
		errors += res.errors
	}

	log.Printf("Processed %d, errors %d", processed, errors)
	if processed == 0 {
		log.Print("Not processed")
		return
	}

	errRate := float64(errors) / float64(processed)
	if errRate < NORMAL_ERR_RATE {
		for _, fn := range processedFiles {
			dotRename(fn)
		}
		log.Printf("Acceptable error rate (%f). Successfull load", errRate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", errRate, NORMAL_ERR_RATE)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.Printf("Work time %s", elapsed)

}
