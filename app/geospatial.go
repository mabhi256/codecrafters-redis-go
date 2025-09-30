package main

func (ss *SortedSet) InsertGeohash(member string, lon, lat float64) {
	score := geohash(lon, lat)
	ss.skipList.Insert(member, score)
	ss.hashmap[member] = score
}

func geohash(lat, lon float64) float64 {
	return 0
}
