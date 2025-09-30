package main

import "math"

func (ss *SortedSet) InsertGeohash(member string, lon, lat float64) {
	score := encodeGeohash(lon, lat)
	ss.skipList.Insert(member, score)
	ss.hashmap[member] = score
}

func GetCoordinates(geohash float64) (float64, float64) {
	return dencodeGeohash(geohash)
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

func dencodeGeohash(geohash float64) (float64, float64) {
	score := uint64(geohash) // we only using 52 bits, so it is safe to do this

	lonMin, lonMax := -180.0, 180.0
	latMin, latMax := -85.05112878, 85.05112878

	// Read bits from MSB (bit 51) to LSB (bit 0)
	for i := 51; i >= 0; i-- {
		bit := (score >> i) & 1

		// Encoding longitude: i=0 (even) created the MSB → Decoding: bit 51 (odd)
		if i%2 == 1 { // longitude bit
			xmid := (lonMin + lonMax) / 2
			if bit == 1 {
				lonMin = xmid // right half
			} else {
				lonMax = xmid
			}
		} else {
			ymid := (latMin + latMax) / 2
			if bit == 1 {
				latMin = ymid // top half
			} else {
				latMax = ymid
			}
		}
	}

	// After 52 bits, we've narrowed to a small rectangle, return its centre
	lon := (lonMin + lonMax) / 2
	lat := (latMin + latMax) / 2

	return lon, lat
}

const rEarth = 6372797.560856 // meters

func haversine(θ float64) float64 {
	return 0.5 * (1 - math.Cos(θ))
}

func radToDeg(radian float64) float64 {
	return radian * math.Pi / 180
}

func hsDist(ψ1, φ1, ψ2, φ2 float64) float64 {
	return 2 * rEarth * math.Asin(math.Sqrt(
		haversine(φ2-φ1)+math.Cos(φ1)*math.Cos(φ2)*haversine(ψ2-ψ1)))
}

func HaversineDist(score1, score2 float64) float64 {

	lon1, lat1 := GetCoordinates(score1) // lon, lat
	lon2, lat2 := GetCoordinates(score2)

	ψ1, φ1 := radToDeg(lon1), radToDeg(lat1)
	ψ2, φ2 := radToDeg(lon2), radToDeg(lat2)

	return hsDist(ψ1, φ1, ψ2, φ2)
}

func (ss *SortedSet) SearchFrom(lon, lat, radius float64) []string {
	ψ1, φ1 := radToDeg(lon), radToDeg(lat)

	result := []string{}
	for member, score := range ss.hashmap {
		lon2, lat2 := GetCoordinates(score)
		ψ2, φ2 := radToDeg(lon2), radToDeg(lat2)

		dist := hsDist(ψ1, φ1, ψ2, φ2)
		if dist <= radius {
			result = append(result, member)
		}
	}

	return result
}
