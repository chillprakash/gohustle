package appparameters

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// SetPnLParameter saves a PnL parameter with enabled/disabled state and value
func (apm *AppParameterManager) SetPnLParameter(ctx context.Context, paramType AppParameterTypes, param PnLParameter) error {
	jsonData, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = apm.SetParameter(ctx, paramType, string(jsonData))
	return err
}

// GetPnLParameter retrieves a PnL parameter with its enabled state and value
func (apm *AppParameterManager) GetPnLParameter(ctx context.Context, paramType AppParameterTypes) (PnLParameter, error) {
	var result PnLParameter
	// Default values
	result.Enabled = false
	result.Value = 0.0

	// Get the parameter
	param, err := apm.GetParameter(ctx, paramType)
	if err != nil {
		if err == redis.Nil {
			// Return default values if parameter doesn't exist
			return result, nil
		}
		return result, err
	}

	// Unmarshal the JSON
	err = json.Unmarshal([]byte(param.Value), &result)
	if err != nil {
		// Try to parse as a plain float value for backward compatibility
		if val, convErr := strconv.ParseFloat(param.Value, 64); convErr == nil {
			result.Value = val
			result.Enabled = true // Assume enabled if it exists as a plain value
			return result, nil
		}
		return result, err
	}

	return result, nil
}

// SetLimitOrderParameter saves a limit order parameter with enabled/disabled state and percentage value
func (apm *AppParameterManager) SetLimitOrderParameter(ctx context.Context, paramType AppParameterTypes, param LimitOrderParameter) error {
	jsonData, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = apm.SetParameter(ctx, paramType, string(jsonData))
	return err
}

// GetLimitOrderParameter retrieves a limit order parameter with its enabled state and percentage value
func (apm *AppParameterManager) GetLimitOrderParameter(ctx context.Context, paramType AppParameterTypes) (LimitOrderParameter, error) {
	var result LimitOrderParameter
	// Default values
	result.Enabled = false
	result.Value = 0

	// Get the parameter
	param, err := apm.GetParameter(ctx, paramType)
	if err != nil {
		if err == redis.Nil {
			// Return default values if parameter doesn't exist
			return result, nil
		}
		return result, err
	}

	// Unmarshal the JSON
	err = json.Unmarshal([]byte(param.Value), &result)
	if err != nil {
		// Try to parse as a plain int value for backward compatibility
		if val, convErr := strconv.Atoi(param.Value); convErr == nil {
			result.Value = val
			result.Enabled = true // Assume enabled if it exists as a plain value
			return result, nil
		}
		return result, err
	}

	return result, nil
}

// GetLimitOrderSettings retrieves the limit order enabled state and percentage in one call
func (apm *AppParameterManager) GetLimitOrderSettings(ctx context.Context) (bool, int, error) {
	// Get limit order status
	limitOrderParam, err := apm.GetLimitOrderParameter(ctx, AppParamLimitOrder)
	if err != nil && err != redis.Nil {
		return false, 0, err
	}

	// Get percentage value
	limitOrderPercent, err := apm.GetLimitOrderParameter(ctx, AppParamLimitOrderPercent)
	if err != nil && err != redis.Nil {
		return limitOrderParam.Enabled, 0, err
	}

	return limitOrderParam.Enabled, limitOrderPercent.Value, nil
}

// SetLimitOrderSettings updates both limit order status and percentage, plus updates order type
func (apm *AppParameterManager) SetLimitOrderSettings(ctx context.Context, enabled bool, percent int) error {
	// Set limit order status
	limitOrderParam := LimitOrderParameter{
		Enabled: enabled,
		Value:   percent,
	}

	// Update limit order parameter
	err1 := apm.SetLimitOrderParameter(ctx, AppParamLimitOrder, limitOrderParam)

	// Update limit order percent parameter
	limitPercentParam := LimitOrderParameter{
		Enabled: enabled,
		Value:   percent,
	}
	err2 := apm.SetLimitOrderParameter(ctx, AppParamLimitOrderPercent, limitPercentParam)

	// Update order type accordingly
	orderType := OrderTypeMARKET
	if enabled {
		orderType = OrderTypeLIMIT
	}
	_, err3 := apm.SetParameter(ctx, AppParamOrderType, string(orderType))

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

// GetPnLSettings retrieves both exit and target PnL settings in one call
func (apm *AppParameterManager) GetPnLSettings(ctx context.Context) (exitEnabled bool, exitValue float64, targetEnabled bool, targetValue float64, err error) {
	// Get exit PnL parameter
	exitPnl, err1 := apm.GetPnLParameter(ctx, AppParamExitPNL)
	if err1 != nil && err1 != redis.Nil {
		return false, 0, false, 0, err1
	}

	// Get target PnL parameter
	targetPnl, err2 := apm.GetPnLParameter(ctx, AppParamTargetPNL)
	if err2 != nil && err2 != redis.Nil {
		return exitPnl.Enabled, exitPnl.Value, false, 0, err2
	}

	return exitPnl.Enabled, exitPnl.Value, targetPnl.Enabled, targetPnl.Value, nil
}

// SetPnLSettings updates both exit and target PnL settings
func (apm *AppParameterManager) SetPnLSettings(ctx context.Context, exitEnabled bool, exitValue float64, targetEnabled bool, targetValue float64) error {
	// Set exit PnL parameter
	exitParam := PnLParameter{
		Enabled: exitEnabled,
		Value:   exitValue,
	}
	err1 := apm.SetPnLParameter(ctx, AppParamExitPNL, exitParam)

	// Set target PnL parameter
	targetParam := PnLParameter{
		Enabled: targetEnabled,
		Value:   targetValue,
	}
	err2 := apm.SetPnLParameter(ctx, AppParamTargetPNL, targetParam)

	if err1 != nil {
		return err1
	}
	return err2
}
