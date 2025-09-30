package main

func (ss *SortedSet) InsertGeohash(member string, lon, lat float64) {
	score := encodeGeohash(lon, lat)
	ss.skipList.Insert(member, score)
	ss.hashmap[member] = score
}

// const base32 = "0123456789bcdefghjkmnpqrstuvwxyz" // (geohash-specific) 0-9, b-z excluding a,i,l,o
//
// ┌─┬──────────┬────────────────────────────────────────────────────┐
// │S│ Exponent │              Mantissa (Significand)                │
// └─┴──────────┴────────────────────────────────────────────────────┘
//
//	1    11 bits                   52 bits
//
// value = (-1)^sign × (1.mantissa) × 2^(exponent - 1023)
//
// float64 can only store 52 bits of mantissa; beyond that we lose precision.
// Usually geohash is 60 bits (12 base32 chars of 5 bits), but redis only uses 52 bit geohash
func encodeGeohash(lon, lat float64) float64 {
	var score uint64 = 0

	//                +90°(N)
	//                   │
	//                   │
	// -180°(W) ─────────┼───────── +180°(E)
	//                   │
	//                	 │
	//                -90°(S)
	//
	lonMin, lonMax := -180.0, 180.0             // left/right of prime meridian
	latMin, latMax := -85.05112878, 85.05112878 // bot/top of equator, web mercator projection

	for i := range 52 {
		if i%2 == 0 { // longitude bit
			xmid := (lonMin + lonMax) / 2
			if lon < xmid { // choose left half, so reduce the right boundary
				score = score << 1 //left = 0
				lonMax = xmid
			} else {
				score = score<<1 | 1 // right = 1
				lonMin = xmid
			}
		} else { // latitude bit
			ymid := (latMin + latMax) / 2
			if lat < ymid { // choose bot half, so reduce the top boundary
				score = score << 1 //bot = 0
				latMax = ymid
			} else {
				score = score<<1 | 1 // top = 1
				latMin = ymid
			}
		}
	}

	return float64(score)
}

func dencodeGeohash(lat, lon float64) (float64, float64) {
	return 0, 0
}
