package main

import (
	"sync"
	"os"
	"log"
	"io"
	"encoding/json"
)

type URLStore struct {
	urls 	map[string]string
	mu 		sync.RWMutex
	save 	chan record
}

type record struct {
	Key string
	URL string
}

func (s *URLStore) load(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		log.Println("Error opening URLStore: ", err)
		return err
	}
	defer f.Close()
	
	d := json.NewDecoder(f) 
	for err == nil {
		var r record
		if err = d.Decode(&r); err == nil {
			log.Println("Loaded: ", r.Key, r.URL)
			s.Set(r.Key, r.URL)
		}
	}
	if err == io.EOF {
		return nil
	}
	log.Println("Error decoding URLStore: ", err)
	return err
}


func (s *URLStore) saveLoop(filename string) {
	f, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("URLStore: ", err)
	}
	defer f.Close()
	
	e := json.NewEncoder(f)
	for {
		r := <-s.save
		if err := e.Encode(r); err != nil {
			log.Println("URLStore: ", err)
		}
		log.Println("Saved: ", r.Key, r.URL)
	}
}

const saveQueueLength = 1000
func NewURLStore(filename string) *URLStore {
	s := &URLStore{ 
			urls: make(map[string]string),
			save: make(chan record, saveQueueLength),
		}
	if err := s.load(filename); err != nil {
		log.Println("Error loading data in URLStore: ", err)
	}
	go s.saveLoop(filename)
	return s
}


func (s *URLStore) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	url := s.urls[key]
	
	return url
}

func (s *URLStore) Set(key, url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	_, present := s.urls[key]
	if present {
		
		return false
	}
	s.urls[key] = url
	return true
}

func (s *URLStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.urls)
}

func (s *URLStore) Put(url string) string {
	for {
		key := genKey(s.Count())
		if s.Set(key, url) {
			s.save <- record{key, url}
			log.Println("Saved key: ", key)
			return key
		}
	}
	panic("Should never reach here")
}