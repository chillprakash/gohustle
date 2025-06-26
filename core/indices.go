package core

// Index represents a market index with its details
type Index struct {
	NameInOptions   string
	InstrumentToken uint32
	NameInIndices   string
	Enabled         bool
	IndexNumber     int // For ordering/rendering in client and server
	StrikeGap       int
	UnitsPerLot     int
	MaxLotsPerOrder int
}

// GetStrikeGap returns the strike gap for this index
func (i Index) GetStrikeGap() int {
	return i.StrikeGap
}

// GetUnitsPerLot returns the units per lot for this index
func (i Index) GetUnitsPerLot() int {
	return i.UnitsPerLot
}

// GetMaxLotsPerOrder returns the max lots per order for this index
func (i Index) GetMaxLotsPerOrder() int {
	return i.MaxLotsPerOrder
}

// Indices represents all available market indices
type Indices struct {
	NIFTY     Index
	SENSEX    Index
	BANKNIFTY Index
}

// GetIndices returns a singleton instance of Indices
var GetIndices = func() Indices {
	return Indices{
		NIFTY: Index{
			NameInOptions:   "NIFTY",
			InstrumentToken: 256265,
			NameInIndices:   "NIFTY 50",
			Enabled:         true,
			IndexNumber:     1, // Primary index
			StrikeGap:       50,
			UnitsPerLot:     1800,
			MaxLotsPerOrder: 75,
		},
		SENSEX: Index{
			NameInOptions:   "SENSEX",
			InstrumentToken: 265,
			NameInIndices:   "SENSEX",
			Enabled:         true,
			IndexNumber:     0, // Secondary index
			StrikeGap:       100,
			UnitsPerLot:     1000,
			MaxLotsPerOrder: 20,
		},
		BANKNIFTY: Index{
			NameInOptions:   "BANKNIFTY",
			InstrumentToken: 260105,
			NameInIndices:   "NIFTY BANK",
			Enabled:         true,
			IndexNumber:     2, // Tertiary index
			StrikeGap:       100,
			UnitsPerLot:     900,
			MaxLotsPerOrder: 35,
		},
	}
}

// GetAllIndices returns a slice of all indices
func (i Indices) GetAllIndices() []Index {
	indices := make([]Index, 0, 3)

	if i.NIFTY.Enabled {
		indices = append(indices, i.NIFTY)
	}
	if i.SENSEX.Enabled {
		indices = append(indices, i.SENSEX)
	}
	if i.BANKNIFTY.Enabled {
		indices = append(indices, i.BANKNIFTY)
	}

	return indices
}

// GetAllInstrumentTokens returns a slice of all instrument tokens
func (i Indices) GetAllInstrumentTokens() []uint32 {
	tokens := make([]uint32, 0, 3)

	if i.NIFTY.Enabled {
		tokens = append(tokens, i.NIFTY.InstrumentToken)
	}
	if i.SENSEX.Enabled {
		tokens = append(tokens, i.SENSEX.InstrumentToken)
	}
	if i.BANKNIFTY.Enabled {
		tokens = append(tokens, i.BANKNIFTY.InstrumentToken)
	}

	return tokens
}

// GetAllNames returns a slice of all index names
func (i Indices) GetAllNames() []string {
	names := make([]string, 0, 3)

	if i.NIFTY.Enabled {
		names = append(names, i.NIFTY.NameInOptions)
	}
	if i.SENSEX.Enabled {
		names = append(names, i.SENSEX.NameInOptions)
	}
	if i.BANKNIFTY.Enabled {
		names = append(names, i.BANKNIFTY.NameInOptions)
	}

	return names
}

// GetIndexByName returns an Index by its name
func (i Indices) GetIndexByName(name string) *Index {
	switch name {
	case "NIFTY":
		if i.NIFTY.Enabled {
			return &i.NIFTY
		}
	case "SENSEX":
		if i.SENSEX.Enabled {
			return &i.SENSEX
		}
	case "BANKNIFTY":
		if i.BANKNIFTY.Enabled {
			return &i.BANKNIFTY
		}
	}
	return nil
}

// GetIndexByExchangeName returns an Index by its exchange name
func (i Indices) GetIndexByExchangeName(exchangeName string) *Index {
	switch exchangeName {
	case "NIFTY 50":
		if i.NIFTY.Enabled {
			return &i.NIFTY
		}
	case "SENSEX":
		if i.SENSEX.Enabled {
			return &i.SENSEX
		}
	case "NIFTY BANK":
		if i.BANKNIFTY.Enabled {
			return &i.BANKNIFTY
		}
	}
	return nil
}

// GetIndicesToSubscribeForIntraday returns all indices for intraday subscription
func (i Indices) GetIndicesToSubscribeForIntraday() []Index {
	indices := make([]Index, 0, 3)

	if i.NIFTY.Enabled {
		indices = append(indices, i.NIFTY)
	}
	if i.SENSEX.Enabled {
		indices = append(indices, i.SENSEX)
	}
	if i.BANKNIFTY.Enabled {
		indices = append(indices, i.BANKNIFTY)
	}

	return indices
}

// GetIndicesToCollectData returns indices for data collection
func (i Indices) GetIndicesToCollectData() []Index {
	indices := make([]Index, 0, 2)

	if i.NIFTY.Enabled {
		indices = append(indices, i.NIFTY)
	}
	if i.SENSEX.Enabled {
		indices = append(indices, i.SENSEX)
	}
	// Note: BANKNIFTY is intentionally excluded from data collection

	return indices
}
