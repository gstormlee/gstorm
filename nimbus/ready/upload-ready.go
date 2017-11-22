package ready

import "github.com/gstormlee/gstorm/nimbus/distribute"

// OnFileUploadReady  func
func OnFileUploadReady(json, storm string) {
	stormData := distribute.StormData{}
	stormData.TopologyFile = json
	stormData.Bin = storm
	data := distribute.GetInstance()
	data.Datas[json] = stormData
	distribute.WaitChanel <- json
}
