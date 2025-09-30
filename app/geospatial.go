package main

type GeoSet struct {
	geohash *SkipList
	lat     map[string]float64
	lon     map[string]float64
}

func (ss *GeoSet) Type() string {
	return "geo"
}

func NewGeoSet() *GeoSet {
	zset := &GeoSet{
		geohash: NewSkipList(),
		lat:     make(map[string]float64),
		lon:     make(map[string]float64),
	}

	return zset
}

func (ss *GeoSet) Insert(member string, lon, lat float64) {
	score := geohash(lon, lat)
	ss.geohash.Insert(member, score)
	ss.lon[member] = lon
	ss.lat[member] = lat
}

func (ss *GeoSet) Remove(member string) {
	lon, exists := ss.lon[member]

	if exists {
		score := geohash(lon, ss.lat[member])
		ss.geohash.Remove(member, score)
		delete(ss.lon, member)
		delete(ss.lat, member)
	}
}

func geohash(lat, lon float64) float64 {
	return 1
}
