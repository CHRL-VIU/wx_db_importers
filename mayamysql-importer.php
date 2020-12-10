
<?php

// This script updates the viu hydromet mysql data base tables for just mt maya, pulls last 12 hours from ftp and updates table
require 'maya_ftp_config.php';
require 'functions.php';
require 'config.php';

# number of rows to grab from tail
define("NUMROWS", 12);

$tbl = "mountmaya";
$numToClean = 30000; // was 12 hr

$ftpFilename = "ftps://".FTPUSER.":".FTPPASS."@".FTPHOST;
$conn = mysqli_connect(MYSQLHOST, MYSQLUSER, MYSQLPASS, MYSQLDB);

$ftpStreamOptions = array(
    "ssl"=>array(
        "verify_peer"=>false,
        "verify_peer_name"=>false,
    ),
);
$ftpStreamContext = stream_context_create($ftpStreamOptions);
$ftpFileArray = file($ftpFilename, false, $ftpStreamContext);

if (mysqli_connect_errno()) {
  echo "Failed to connect to MySQL: " . mysqli_connect_error();
  exit;
}

if (!$ftpFileArray) {
  echo "Error connecting to FTPS filename $ftpFilename";
  exit;
}


# create array of data from FTP only grab num rows we need. 
$fields = "DateTime,RECORD,BattV_Min,BattV_Max,BattV_Avg,PTemp_C_Avg,PTemp_C_Min,PTemp_C_Max,EnclosureTemp_Avg,
EnclosureTemp_Min,EnclosureTemp_Max,AirTC,AirTC_Avg,AirTC_Std,AirTC_Min,AirTC_Max,RH,RH_Avg,RH_Std,RH_Min,RH_Max,
DT,DT_Avg,DT_Std,DT_Min,DT_Max,Q,Q_Avg,Q_Std,Q_Min,Q_Max,TCDT,TCDT_Avg,TCDT_Std,TCDT_Min,TCDT_Max,DBTCDT,DBTCDT_Avg,
DBTCDT_Std,DBTCDT_Min,DBTCDT_Max,WS_ms_Avg,WS_ms_Std,WS_ms_Min,WS_ms_Max,WindDir,WS_ms_S_WVT,WindDir_D1_WVT,WindDir_SD1_WVT,
Rain_mm_Tot,BaroP,BaroP_Avg,BaroP_Std,BaroP_Min,BaroP_Max,SolarRad,SolarRad_Avg,SolarRad_Std,SolarRad_Min,SolarRad_Max,
PrecipGaugeLvl,PrecipGaugeLvl_Avg,PrecipGaugeLvl_Std,PrecipGaugeLvl_Min,PrecipGaugeLvl_Max,PrecipGaugeTemp,
PrecipGaugeTemp_Avg,PrecipGaugeTemp_Std,PrecipGaugeTemp_Min,PrecipGaugeTemp_Max,AirTC2,AirTC2_Avg,AirTC2_Std,AirTC2_Min,AirTC2_Max";

# get tail of ftp data. still has to load whole file first 
$csv = array_slice(array_map('str_getcsv', $ftpFileArray), -NUMROWS);

$lines = 0;

foreach ($csv as $line) {
       
    $lines++;

    $linemysql = implode("','",$line);

    // use the first entry of the linearray array to find the appropriate table. 
    $query = "insert ignore into `raw_$tbl` ($fields) values('$linemysql');";

    if (!mysqli_query($conn, $query)) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }
}

// Then update clean table //
// temp change
function getMySQLRows2($conn, $stationName, $numRows) {

    $sql = "(SELECT * FROM `$stationName` ORDER BY DateTime asc LIMIT $numRows) order by DateTime";

    $result = mysqli_query($conn,$sql);

    if(!$result){
        exit("Select Query Error Description: ". mysqli_error($conn));
    }

    // put query in assoc array 
    $raw_array = array();

    while($row = mysqli_fetch_assoc($result)) {
        $raw_array[] = $row;
        }

    return $raw_array;
}

// get rows from mysql
$rawRows = getMySQLRows2($conn, "raw_$tbl", $numToClean);





$lineNum = 0;
foreach ($rawRows as $line) {
    ///// calcs //////
    if ($lineNum == 0) {
        //$PP_Pipe = 0;
        $prevPCraw = $line["PrecipGaugeLvl_Avg"];
        $lineNum++;
        // need to skip  line on first row so PP_pipe not set to 0
        continue;
    } else {
        $PP_Pipe = ($line["PrecipGaugeLvl_Avg"] - $prevPCraw) * 1000;
    }

    // store current pc_raw val for next row    
    $prevPCraw = $line["PrecipGaugeLvl_Avg"];
    $lineNum++;

    $curDateTime = $line["DateTime"];
    $curWatYr = wtr_yr($curDateTime, 10); // calc wat yr

    $cleanRow = array(
        "DateTime" => $line["DateTime"],
        "WatYr" =>  $curWatYr,
        "Air_Temp" => $line["AirTC_Avg"],
        "Rh" => $line["RH_Avg"],
        "BP" => $line["BaroP_Avg"],
        "Wind_Speed" => $line["WS_ms_Avg"] * 3.6,
        "Wind_Dir" => ($line["WindDir_D1_WVT"]>=180 ? $line["WindDir_D1_WVT"]-=180 : $line["WindDir_D1_WVT"] +=180), // rm young is backwards on tower
        "Pk_Wind_Speed" => $line["WS_ms_Max"] * 3.6,
        "PP_Tipper" => $line["Rain_mm_Tot"],
        "PC_Raw_Pipe" => $line["PrecipGaugeLvl_Avg"] * 1000,
        "PP_Pipe" => $PP_Pipe,
        "Snow_Depth" => ((3.8 - $line["TCDT_Avg"]) * 100), // distance to ground processed on unit
        "Solar_Rad" => $line["SolarRad_Avg"],
        "Batt" => $line["BattV_Avg"]
    );

    if (count($cleanRow) > 0) {
        $fields = implode(", ", array_keys($cleanRow));
        $values = implode("','", array_values($cleanRow));
    }

    $query = "UPDATE `clean_$tbl` SET WatYr = $curWatYr WHERE DateTime = '$curDateTime'";
    //$query = "INSERT IGNORE into `clean_$tbl` ($fields) values('$values')";


    if (!mysqli_query($conn, $query)) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }
}

mysqli_close($conn);

?>