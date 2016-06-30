package main

import (
	"sync"
	"os"
	"log"
	"io"
	"encoding/gob"
)

type URLStore struct {
	urls 	map[string]string
	mu 		sync.RWMutex
	file 	*os.File
}

type record struct {
	Key string
	URL string
}

func (s *URLStore) load() error {
	if _, err := s.file.Seek(0, 0); err != nil {
		return err
	}
	d := gob.NewDecoder(s.file) 
	var err error
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
	return err
}

func (s *URLStore) save(key, url string) error {
	e := gob.NewEncoder(s.file)
	log.Println("Saving: ", key, url)
	return e.Encode(record{key, url})
}

func NewURLStore(filename string) *URLStore {
	s := &URLStore{ urls: make(map[string]string) }
	f, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("URLStore: ", err)
	}
	s.file = f
	if err := s.load(); err != nil {
		log.Println("Error loading data in URLStore: ", err)
	}
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
			if err := s.save(key, url); err != nil {
				log.Println("Error saving to URLStore")
			}
			log.Println("Saved key: ", key)
			return key
		}
	}
	panic("Should never reach here")
}