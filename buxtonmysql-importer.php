<?php
// query's html file from hakai live website grabs javascript table.
require 'config.php';
require 'functions.php';

$tbl = "eastbuxton";
$file_path = '/home/viuhydro/data/eastbuxton/EastRidgeWx_OneHour.dat'; // update 2023-12-01 by alex from original html scrape here 'https://hecate.hakai.org/sn/1hourSamples/last4weeks-BuxtonEast.1hourSamples.html'
$maxRows = -24; // negative to grab end of the file
$field_row_num = 1; // the row num of the raw input fields
$viu_raw_keys = "DateTime,Air_Temp,Relative_Humidity,Snow_Depth,Wind_Spd,Wind_Dir,Air_Pressure,SolarRad_Avg,SolarRad_24hr,Rain,Rain_24hr,Pcp_GaugeLvl,Pcp_GaugeTemp,EnclosureTemp,Panel_Temp,BattVolt";

if (!file_exists($file_path)) {
    echo "File not found. Make sure you specified the correct path.\n";
    exit;
}

$conn = mysqli_connect(MYSQLHOST, MYSQLUSER, MYSQLPASS, MYSQLDB);

if (mysqli_connect_errno())
  {
  echo "Failed to connect to MySQL: " . mysqli_connect_error();
  }

$file = file($file_path);

$data = array_slice($file, $maxRows);

$hakai_fields = str_getcsv($file[1], ',', '"');

# loop through each line in the $data array number of lines can be controlled by the max rows param
foreach ($data as $line) {

    $datArray = str_getcsv($line, ',', '"');

    $datArray = array_combine($hakai_fields, $datArray);

    # format datetime to mysql time format, time is PST on garrison 
    $datArray['TIMESTAMP'] = date("Y-m-d H:i:s", strtotime($datArray['TIMESTAMP'])); 

    $dataRow=array(
        'DateTime'=>$datArray['TIMESTAMP'],
        'Air_Temp'=>$datArray['AirTemp_Avg'],
        'Relative_Humidity'=>$datArray['RH_Avg'],
        'Snow_Depth'=>$datArray['SR50A_TC_Distance_Avg'], // in metres
        'Wind_Spd'=>$datArray['WindSpd_Avg'], // in m/s
        'Wind_Dir'=>$datArray['WindDir_Avg'], // rm young is pointed north, so need to adjust 180 deg later 
        'Air_Pressure'=>$datArray['StationAirPressure_Avg'], // in hpa
        'SolarRad_Avg'=>$datArray['SolarRad_Avg'],
        'SolarRad_24hr'=>$datArray['SolarRad_Tot_Tot'],
        'Rain'=>$datArray['Rain_mm_Tot'],
        'Rain_24hr'=>$datArray['RainToday'],
        'Pcp_GaugeLvl'=>$datArray['PrecipGaugeLvl'], // in metres
        'Pcp_GaugeTemp'=>$datArray['PrecipGaugeTemp'], // not averaged, inst.
        'EnclosureTemp'=>$datArray['EnclosureTemp_Avg'],
        'Panel_Temp'=>$datArray['PanelT_Avg'],
        'BattVolt'=>$datArray['BattVolt_Avg']
    );
    # back to string format for mysql ingest
    $datString = implode("','", $dataRow);

    // use the first entry of the linearray array to find the appropriate table. 
     $query = "insert ignore into `raw_$tbl` ($viu_raw_keys) values('$datString');";

if (!mysqli_query($conn, $query)) {
    exit("Insert Query Error description: " . mysqli_error($conn));
}
}

// Then update clean table //

// get rows from mysql
$rawRows = getMySQLRows($conn, "raw_$tbl", abs($maxRows));

$lineNum = 0;
foreach ($rawRows as $line) {
    ///// calcs //////
    if ($lineNum == 0) {
        //$PP_Pipe = 0;
        $prevPCraw = $line["Pcp_GaugeLvl"];
        $lineNum++;
        // need to skip  line on first row so PP_pipe not set to 0
        continue;
    } else {
        $PP_Pipe = ($line["Pcp_GaugeLvl"] - $prevPCraw) * 1000;
    }

    // store current pc_raw val for next row
    $prevPCraw = $line["Pcp_GaugeLvl"];
    $lineNum++;

    $curDateTime = $line["DateTime"];
    $curWatYr = wtr_yr($curDateTime, 10); // calc wat yr

    $cleanRow = array(
        "DateTime" => $line["DateTime"],
        "WatYr" =>  $curWatYr,
        "Air_Temp" => $line["Air_Temp"],
        "Rh" => $line["Relative_Humidity"],
        "BP" => $line["Air_Pressure"],
        "Wind_Speed" => $line["Wind_Spd"] * 3.6, // to km/hr
        "Wind_Dir" => ($line["Wind_Dir"] >= 180 ? $line["Wind_Dir"] -= 180 : $line["Wind_Dir"] += 180), // rm young is backwards on tower
        "PP_Tipper" => $line["Rain"],
        "PC_Raw_Pipe" => $line["Pcp_GaugeLvl"] * 1000,
        "PP_Pipe" => $PP_Pipe,
        "Snow_Depth" => ($line["Snow_Depth"] == 0 ? NAN : (3.66 - $line["Snow_Depth"]) * 100), // Check and adjust snow depth
        "Solar_Rad" => $line["SolarRad_Avg"],
        "Batt" => $line["BattVolt"]
    );

    if (count($cleanRow) > 0) {
        $fields = implode(", ", array_keys($cleanRow));
        $values = implode("','", array_values($cleanRow));
    }

    //$query = "UPDATE `clean_$tbl` SET WatYr = $curWatYr WHERE DateTime = '$curDateTime'";
    $query = "INSERT IGNORE into `clean_$tbl` ($fields) values('$values')";

   if (!mysqli_query($conn, $query)) {
        exit("Clean Table Insert Error description: " . mysqli_error($conn));
    }
}
mysqli_close($conn);
?>
