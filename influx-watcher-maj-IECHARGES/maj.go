package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	influx "github.com/influxdata/influxdb1-client/v2"
)

var (
	influxHost       = getEnv("INFLUX_HOST", "tsdbe.nidec-asi-online.com")
	influxPort       = getEnv("INFLUX_PORT", "443")
	influxUser       = getEnv("INFLUX_USER", "nw")
	influxPw         = getEnv("INFLUX_PW", "at3Dd94Yp8BT4Sh!")
	influxDB         = getEnv("INFLUX_DB", "signals")
	influxMeas       = getEnv("INFLUX_MEAS", "fastcharge")
	influxTagProject = getEnv("INFLUX_TAG_PROJECT", "project")

	mysqlDSN = getEnv("MYSQL_DSN", "nidec:MaV38f5xsGQp83@tcp(162.19.251.55:3306)/Charges?parseTime=true")

	projects = []string{
		"7571", "7796", "7797", "7798", "7800", "7803", "7804", "7809", "7812", "7813", "7814", "7818", "7819", "7825", "7828", "7833",
		"7951-001", "7951-003", "7951-050", "7951-051", "7951-054", "7951-057", "7951-062", "7951-063", "7951-065",
		"7951-067", "7951-071", "7951-079", "7951-081", "7951-083", "7951-085", "7951-086", "7951-087", "7951-088",
		"7951-091", "7951-093", "7951-094", "7951-096", "7951-099", "7951-100", "7951-108", "7951-112", "7951-114",
		"7951-115", "7951-118", "7951-121", "7951-122", "7951-124", "7951-125", "7951-128", "7951-130", "7951-131",
		"7951-134", "7951-135", "7951-139", "7951-142", "7951-149",
		"8266-156", "8266-160", "8266-161", "8266-163", "8266-165", "8266-166", "8266-167", "8266-168", "8266-174",
		"8266-179", "8266-184", "8266-185", "8266-187", "8266-191", "8266-196", "8266-197", "8266-199", "8266-203",
		"8266-208", "8266-209", "8266-210", "8266-211", "8266-214", "8266-217", "8266-218", "8266-221", "8266-222",
		"8266-223", "8266-227", "8266-230", "8266-234", "8266-240", "8266-246", "8266-247", "8266-250", "8266-254",
		"8266-259", "8266-266", "8266-269", "8266-272", "8266-273", "8266-274",
		"8558-276", "8558-281", "8558-282", "8558-283", "8558-289", "8558-292", "8558-301", "8558-304", "8558-311",
		"8558-313", "8558-314", "8558-317", "8558-318", "8558-320", "8558-321", "8558-322", "8558-324", "8558-328",
		"8558-330", "8558-336", "8558-337", "8558-339", "8558-340",
	}

	mappingSites = map[string]string{
		"7571":     "Orignolles",
		"7796":     "Meru",
		"7797":     "Charleval",
		"7798":     "Triel",
		"7800":     "Saujon",
		"7803":     "Cierzac",
		"7804":     "Os Marsillon",
		"7809":     "St Pere en retz",
		"7812":     "Hagetmau",
		"7813":     "Biscarosse",
		"7814":     "Auriolles",
		"7818":     "Verneuil",
		"7819":     "Allaire",
		"7825":     "Vezin",
		"7828":     "Pontchateau",
		"7833":     "Pontfaverger",
		"7951-001": "Baud",
		"7951-003": "Maurs",
		"7951-050": "Mezidon",
		"7951-051": "Derval",
		"7951-054": "Campagne",
		"7951-057": "Mailly le Chateau",
		"7951-062": "Winnezeele",
		"7951-063": "Diges",
		"7951-065": "Vernouillet",
		"7951-067": "Orbec",
		"7951-071": "St Renan",
		"7951-079": "Molompize",
		"7951-081": "Carquefou",
		"7951-083": "Vaupillon",
		"7951-085": "Pleumartin",
		"7951-086": "Caumont sur Aure",
		"7951-087": "Getigne",
		"7951-088": "Chinon",
		"7951-091": "La Roche sur Yon",
		"7951-093": "Aubigne sur Layon",
		"7951-094": "Bonvillet",
		"7951-096": "Rambervillers",
		"7951-099": "Blere",
		"7951-100": "Plouasne",
		"7951-108": "Champniers",
		"7951-112": "Nissan Lez Enserune",
		"7951-114": "Combourg",
		"7951-115": "Vimoutiers",
		"7951-118": "Beaumont de Lomagne",
		"7951-121": "Sueves",
		"7951-122": "Maen Roch",
		"7951-124": "St Leon sur L Isle",
		"7951-125": "Mirecourt",
		"7951-128": "La Voge les Bains",
		"7951-130": "Amanvillers",
		"7951-131": "Guerlesquin",
		"7951-134": "Guerande",
		"7951-135": "Riscle",
		"7951-139": "Avrille",
		"7951-142": "Domfront",
		"7951-149": "Couesmes",
		"8266-156": "Ste Catherine",
		"8266-160": "Andel",
		"8266-161": "Chazey Bons",
		"8266-163": "Lauzerte",
		"8266-165": "Trie la ville",
		"8266-166": "Hambach",
		"8266-167": "Beaugency",
		"8266-168": "Carcassonne",
		"8266-174": "Sable sur Sarthe",
		"8266-179": "Taden",
		"8266-184": "Rue",
		"8266-185": "Quevilloncourt",
		"8266-187": "St Victor de Morestel",
		"8266-191": "St Hilaire du Harcouet",
		"8266-196": "Hémonstoir",
		"8266-197": "Amily",
		"8266-199": "Henrichemont",
		"8266-203": "Couleuvre",
		"8266-208": "St Pierre le Moutier 2",
		"8266-209": "Bourbon L Archambaut",
		"8266-210": "Brou",
		"8266-211": "Neulise",
		"8266-214": "St Jean le vieux",
		"8266-217": "Periers",
		"8266-218": "Quievrecourt",
		"8266-221": "Chazelle sur Lyon",
		"8266-222": "Montverdun",
		"8266-223": "Dormans",
		"8266-227": "Glonville 2",
		"8266-230": "Montalieu Vercieu",
		"8266-234": "Nesle Normandeuse",
		"8266-240": "Noyal Pontivy",
		"8266-246": "Vitre 2",
		"8266-247": "St Amour",
		"8266-250": "Dourdan",
		"8266-254": "Roanne",
		"8266-259": "Plufur",
		"8266-266": "Boinville en Mantois",
		"8266-269": "Loche",
		"8266-272": "Bonnieres sur Seine",
		"8266-273": "Piffonds",
		"8266-274": "St Benin d Azy",
		"8558-276": "Niort St Florent",
		"8558-281": "Chauffailles",
		"8558-282": "St Vincent d Autejac",
		"8558-283": "Culhat",
		"8558-289": "Loireauxence",
		"8558-292": "Reuil",
		"8558-301": "Coteaux sur Loire",
		"8558-304": "Le Mans 2",
		"8558-311": "Chantrigne",
		"8558-313": "St Thelo",
		"8558-314": "St Pierre la cour",
		"8558-317": "Nievroz",
		"8558-318": "Val Revermont",
		"8558-320": "Mondoubleau",
		"8558-321": "Kernoues",
		"8558-322": "Yvetot Bocage",
		"8558-324": "Douchy Montcorbon",
		"8558-328": "Sully sur Loire B",
		"8558-330": "Vincey",
		"8558-336": "Ville en Vermois",
		"8558-337": "Virandeville",
		"8558-339": "Reims",
		"8558-340": "Reims B",
	}
)

var (
	IC1_SEQ02_MAP = map[int]string{
		0: "IC00 - DC contactor line open",
		1: "IC01 - DC Preload contactor open",
		2: "IC02 - DC Preload Fuse",
		3: "IC03 - Battery bank connected",
		4: "IC04 - Inverter not energize",
	}

	PC1_SEQ02_MAP = map[int]string{
		0:  "PC00 - RIO no fault communication",
		1:  "PC01 - Battery bank no fault communication",
		2:  "PC02 - Preload fuses",
		3:  "PC03 - Discordance DC line contactor",
		4:  "PC04 - Discordance Preload contactor",
		5:  "PC05 - No Time OUT",
		6:  "PC06 - Inverter ready",
		7:  "PC07 - Upstream PDC connected",
		8:  "PC08 - MeasVDC > 670 VDC",
		9:  "PC09 - Tilt sensor FLT",
		10: "PC10 - External Emergency stop",
	}

	IC1_SEQ03_MAP = map[int]string{
		0: "IC00 - DC contactor line open",
		1: "IC01 - DC Preload contactor open",
		2: "IC02 - DC Preload Fuse",
		3: "IC03 - Battery bank connected",
		4: "IC04 - Inverter not energize",
	}

	PC1_SEQ03_MAP = map[int]string{
		0:  "PC00 - RIO no fault communication",
		1:  "PC01 - Battery bank no fault communication",
		2:  "PC02 - Preload fuses",
		3:  "PC03 - Discordance DC line contactor",
		4:  "PC04 - Discordance Preload contactor",
		5:  "PC05 - No Time OUT",
		6:  "PC06 - Inverter ready",
		7:  "PC07 - Upstream PDC connected",
		8:  "PC08 - MeasVDC > 670 VDC",
		9:  "PC09 - Tilt sensor FLT",
		10: "PC10 - External Emergency stop",
	}

	PDC_SEQ_IC_MAP = map[int]string{
		0:  "IC00 : Main sequence running",
		1:  "IC01 : Ev contactor not closed",
		2:  "IC02 : No over temp Self",
		3:  "IC03 : CB line close",
		6:  "IC06 : OCPP Running",
		7:  "IC07 : HMI communication",
		8:  "IC08 : No Open OCPP TR",
		9:  "IC09 : DCBM COM",
		10: "IC10 : PDC unvailable from CPO",
		11: "IC11 : Power limit JBOX = 0",
	}

	PDC_SEQ_PC_MAP = map[int]string{
		0:  "PC00 : RIO COm",
		1:  "PC01 : CB line close",
		2:  "PC02 : Inverter M1 Ready",
		3:  "PC03 : UpstreamSequence no fault",
		4:  "PC04 : Ev contactor no discordance",
		6:  "PC06 : No over temp Self",
		7:  "PC07 : No TO",
		8:  "PC08 : Plug no Over Temp CCS",
		9:  "PC09 : Over voltage",
		12: "PC12 : Communication EVI",
		13: "PC13 : EVI Emergency stop",
		14: "PC14 : Manu indispo",
	}
)

type EquipConfig struct {
	ICField string
	PCField string
	ICMap   map[int]string
	PCMap   map[int]string
	Title   string
	EqpName string
}

var equipConfigs = map[string]EquipConfig{
	"DC1": {
		ICField: "SEQ02.OLI.A.IC1",
		PCField: "SEQ02.OLI.A.PC1",
		ICMap:   IC1_SEQ02_MAP,
		PCMap:   PC1_SEQ02_MAP,
		Title:   "Batterie DC1 (SEQ02)",
		EqpName: "Variateur HC1",
	},
	"DC2": {
		ICField: "SEQ03.OLI.A.IC1",
		PCField: "SEQ03.OLI.A.PC1",
		ICMap:   IC1_SEQ03_MAP,
		PCMap:   PC1_SEQ03_MAP,
		Title:   "Batterie DC2 (SEQ03)",
		EqpName: "Variateur HC2",
	},
	"PDC1": {
		ICField: "SEQ12.OLI.A.IC1",
		PCField: "SEQ12.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 1 (SEQ12)",
		EqpName: "PDC1",
	},
	"PDC2": {
		ICField: "SEQ22.OLI.A.IC1",
		PCField: "SEQ22.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 2 (SEQ22)",
		EqpName: "PDC2",
	},
	"PDC3": {
		ICField: "SEQ13.OLI.A.IC1",
		PCField: "SEQ13.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 3 (SEQ13)",
		EqpName: "PDC3",
	},
	"PDC4": {
		ICField: "SEQ23.OLI.A.IC1",
		PCField: "SEQ23.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 4 (SEQ23)",
		EqpName: "PDC4",
	},
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getSiteName(siteCode string) string {
	if name, ok := mappingSites[siteCode]; ok {
		return name
	}
	return siteCode
}

func decodeBinary(value int) []int {
	var positions []int
	for i := 0; i < 32; i++ {
		if value&(1<<i) != 0 {
			positions = append(positions, i)
		}
	}
	return positions
}

func getLatestValue(client influx.Client, siteCode, fieldName string) (float64, error) {
	queryStr := fmt.Sprintf(
		`SELECT "%s" FROM "%s" WHERE "%s" = '%s' ORDER BY time DESC LIMIT 1`,
		fieldName,
		influxMeas,
		influxTagProject,
		siteCode,
	)

	q := influx.NewQuery(queryStr, influxDB, "")
	response, err := client.Query(q)
	if err != nil {
		return 0, err
	}
	if response.Error() != nil {
		return 0, response.Error()
	}

	if len(response.Results) == 0 || len(response.Results[0].Series) == 0 {
		return 0, fmt.Errorf("aucune donnée")
	}

	series := response.Results[0].Series[0]
	if len(series.Values) == 0 {
		return 0, fmt.Errorf("aucune valeur")
	}

	row := series.Values[0]
	if row[1] == nil {
		return 0, fmt.Errorf("valeur NULL")
	}

	var value float64
	switch v := row[1].(type) {
	case float64:
		value = v
	case int64:
		value = float64(v)
	case json.Number:
		if f, err := v.Float64(); err == nil {
			value = f
		} else {
			return 0, fmt.Errorf("impossible de parser")
		}
	default:
		return 0, fmt.Errorf("type inattendu")
	}

	return value, nil
}

func closeDefaut(db *sql.DB, id int, dateFin time.Time) error {
	query := `UPDATE kpi_defauts_log SET date_fin = ? WHERE id = ?`
	_, err := db.Exec(query, dateFin, id)
	if err != nil {
		return fmt.Errorf("erreur fermeture: %w", err)
	}
	log.Printf("  ✓ FERME - ID: %d, Date_Fin: %s", id, dateFin.Format("2006-01-02 15:04:05"))
	return nil
}

func createDefaut(db *sql.DB, site, fieldName, eqp, defaut string, bitPos int, startTime time.Time) error {
	query := `INSERT INTO kpi_defauts_log (site, date_debut, date_fin, defaut, eqp, bit_position, field_name) 
	          VALUES (?, ?, NULL, ?, ?, ?, ?)`
	_, err := db.Exec(query, site, startTime, defaut, eqp, bitPos, fieldName)
	if err != nil {
		return fmt.Errorf("erreur création: %w", err)
	}
	log.Printf("  ✓ CREE - Bit: %d, Defaut: %s, Date_debut: %s", bitPos, defaut, startTime.Format("2006-01-02 15:04:05"))
	return nil
}

func findDefautStartDate(client influx.Client, siteCode, fieldName string, bitPos int) (time.Time, error) {
	lookbackDays := 30
	startTime := time.Now().AddDate(0, 0, -lookbackDays)

	queryStr := fmt.Sprintf(
		`SELECT "%s" FROM "%s" WHERE "%s" = '%s' AND time >= '%s' ORDER BY time ASC`,
		fieldName,
		influxMeas,
		influxTagProject,
		siteCode,
		startTime.Format(time.RFC3339),
	)

	q := influx.NewQuery(queryStr, influxDB, "")
	response, err := client.Query(q)
	if err != nil {
		return time.Time{}, err
	}
	if response.Error() != nil {
		return time.Time{}, response.Error()
	}

	bitMask := 1 << bitPos
	previousValue := 0
	var transitionTime time.Time

	for _, result := range response.Results {
		for _, series := range result.Series {
			for _, row := range series.Values {
				t, err := time.Parse(time.RFC3339, row[0].(string))
				if err != nil {
					continue
				}

				if row[1] == nil {
					continue
				}

				var value int
				switch v := row[1].(type) {
				case float64:
					value = int(v)
				case int64:
					value = int(v)
				case json.Number:
					if f, err := v.Float64(); err == nil {
						value = int(f)
					} else {
						continue
					}
				default:
					continue
				}

				bitActive := (value & bitMask) != 0
				wasBitActive := (previousValue & bitMask) != 0

				if !wasBitActive && bitActive {
					transitionTime = t
				}

				previousValue = value
			}
		}
	}

	if transitionTime.IsZero() {
		return time.Time{}, fmt.Errorf("pas trouvé")
	}

	return transitionTime, nil
}

func processFieldMonitor(client influx.Client, db *sql.DB, siteCode, site string, fieldName string, fieldMap map[int]string, eqpName string) error {
	currentValue, err := getLatestValue(client, siteCode, fieldName)
	if err != nil {
		return fmt.Errorf("erreur récupération valeur: %w", err)
	}
	intValue := int(currentValue)

	log.Printf("    1️⃣ Fermeture des défauts ouverts...")
	queryOpen := `SELECT id, bit_position FROM kpi_defauts_log WHERE site = ? AND field_name = ? AND eqp = ? AND date_fin IS NULL`
	rows, err := db.Query(queryOpen, site, fieldName, eqpName)
	if err != nil {
		return fmt.Errorf("erreur requête: %w", err)
	}
	defer rows.Close()

	closedCount := 0
	for rows.Next() {
		var id, bitPos int
		if err := rows.Scan(&id, &bitPos); err != nil {
			log.Printf("      ✗ Erreur scan: %v", err)
			continue
		}

		bitMask := 1 << bitPos
		bitActive := (intValue & bitMask) != 0

		if !bitActive {
			if err := closeDefaut(db, id, time.Now()); err != nil {
				log.Printf("      ✗ Erreur fermeture: %v", err)
			} else {
				closedCount++
			}
		}
	}

	if closedCount > 0 {
		log.Printf("      → %d défaut(s) fermé(s)", closedCount)
	} else {
		log.Printf("      ℹ Aucun défaut à fermer")
	}

	log.Printf("    2️⃣ Recherche des nouveaux défauts...")
	activeBits := decodeBinary(intValue)

	newCount := 0
	for _, bitPos := range activeBits {
		if defautDesc, ok := fieldMap[bitPos]; ok && defautDesc != "" {
			queryCheck := `SELECT COUNT(*) FROM kpi_defauts_log WHERE site = ? AND field_name = ? AND eqp = ? AND bit_position = ?`
			var count int
			err := db.QueryRow(queryCheck, site, fieldName, eqpName, bitPos).Scan(&count)
			if err != nil {
				log.Printf("      ✗ Erreur vérification: %v", err)
				continue
			}

			if count == 0 {
				startTime, err := findDefautStartDate(client, siteCode, fieldName, bitPos)
				if err != nil {
					log.Printf("      ⚠ Date_debut non trouvée pour bit %d, utilisation NOW (%v)", bitPos, err)
					startTime = time.Now()
				}

				if err := createDefaut(db, site, fieldName, eqpName, defautDesc, bitPos, startTime); err != nil {
					log.Printf("      ✗ Erreur création: %v", err)
				} else {
					newCount++
				}
			}
		}
	}

	if newCount > 0 {
		log.Printf("      → %d nouveau(x) défaut(s)", newCount)
	} else {
		log.Printf("      ℹ Aucun nouveau défaut")
	}

	return nil
}

func processEquipment(client influx.Client, db *sql.DB, siteCode, siteName string, config EquipConfig) error {
	log.Printf("   %s", config.EqpName)

	if err := processFieldMonitor(client, db, siteCode, siteName, config.ICField, config.ICMap, config.EqpName); err != nil {
		log.Printf("    ✗ IC: %v", err)
	}

	if err := processFieldMonitor(client, db, siteCode, siteName, config.PCField, config.PCMap, config.EqpName); err != nil {
		log.Printf("    ✗ PC: %v", err)
	}

	return nil
}

func processSite(client influx.Client, db *sql.DB, siteCode string) error {
	siteName := getSiteName(siteCode)
	log.Printf("\n %s", siteName)

	for _, config := range equipConfigs {
		if err := processEquipment(client, db, siteCode, siteName, config); err != nil {
			log.Printf("  ✗ Erreur: %v", err)
		}
	}

	return nil
}

func main() {
	log.Println("\n ONGOING MONITOR")

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("✗ MySQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("✗ Connexion MySQL: %v", err)
	}
	log.Println("✓ MySQL OK")

	influxURL := fmt.Sprintf("https://%s:%s", influxHost, influxPort)
	client, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:               influxURL,
		Username:           influxUser,
		Password:           influxPw,
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Fatalf("✗ InfluxDB: %v", err)
	}
	defer client.Close()

	log.Printf("✓ InfluxDB OK\n")

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10)

	for _, siteCode := range projects {
		wg.Add(1)
		go func(sc string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := processSite(client, db, sc); err != nil {
				log.Printf("✗ Site %s: %v", sc, err)
			}
		}(siteCode)
	}

	wg.Wait()
}
