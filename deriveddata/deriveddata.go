package deriveddata

import (
	"context"
	"fmt"
	"gohustle/zerodha"
	"math"
	"sort"
)

func PopulateDerivedDataForNifty() {
	kc := zerodha.GetKiteConnect()

	// Print the strike cache
	kc.PrintStrikeCache()

	ltp, err := kc.GetIndexLTPFromRedis(context.Background(), "NIFTY")
	if err != nil {
		return
	}
	fmt.Println(ltp)
	nearestRound := getNearestRound(ltp, 50)
	fmt.Println(nearestRound)
	strikes := getStrikesOnEitherSide(int16(nearestRound), 10, 50)
	fmt.Println(strikes)
	strikeStrings := make([]string, len(strikes))
	for i, strike := range strikes {
		strikeStrings[i] = fmt.Sprintf("%d", strike)
	}
	fmt.Println(strikeStrings)
	tokenMap := zerodha.GetKiteConnect().GetInstrumentInfoWithStrike(strikeStrings)
	fmt.Println(tokenMap)
}

func getNearestRound(number float64, base float64) float64 {
	remainder := math.Mod(number, base)
	if remainder >= base/2 {
		return number + base - remainder
	}
	return number - remainder
}

func getStrikesOnEitherSide(strike int16, count int, interval int) []int16 {
	strikes := make([]int16, 0, count*2)

	for i := 0; i < count; i++ {
		strikes = append(strikes, strike-int16(i*interval))
	}

	for i := 1; i <= count; i++ {
		strikes = append(strikes, strike+int16(i*interval))
	}

	sort.Slice(strikes, func(i, j int) bool {
		return strikes[i] < strikes[j]
	})

	return strikes
}
