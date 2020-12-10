<?php
// This script updates the viu hydromet mysql data base tables for the just steph 3
require 'config.php';
include 'functions.php';

$curStation = "rennellpass";

// number of rows to append from data garrison
$maxRows = -6;
$url = "https://datagarrison.com/users/300234010412670/300234010412670/temp/300234010412670_live.txt";
$fields = "DateTime, Solar_Rad, Snow_Depth, Rain, Air_Temp, RH, Wind_Speed, Gust_Speed, Wind_Dir";

// number of rows to update for clean table
$numToClean = 45000;

$conn = mysqli_connect(MYSQLHOST, MYSQLUSER, MYSQLPASS, MYSQLDB);

if (mysqli_connect_errno()) {
    echo "Failed to connect to MySQL: " . mysqli_connect_error();
}

if (filter_var($url, FILTER_VALIDATE_URL) ==FALSE) {
    echo("$url is not a valid URL");
}

// pull live data from garrison site
$data = array_slice(file($url), $maxRows);

# loop through each line in the $data array number of lines can be controlled by the max rows param
foreach ($data as $line) {
        # remove new line character from line
        $line = str_replace("\r\n", "", $line);

        # sep by tabs
        $datArray = explode("\t",$line);

        # format datetime to mysql time format, time is PST on garrison 
        $datArray[0] = date("Y-m-d H:i:s", strtotime($datArray[0])); 

        # rearange array to match mysql columns, note that col names are wrong on data garrison
        $datArray = array($datArray[0], $datArray[1], $datArray[2], $datArray[3], $datArray[7], $datArray[8], $datArray[4], $datArray[5], $datArray[6]);

        # back to string format for mysql ingest
        $datString = implode("','", $datArray);

        // use the first entry of the linearray array to find the appropriate table. 
         $query = "insert ignore into `raw_$curStation` ($fields) values('$datString');";

    if (!mysqli_query($conn, $query)) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }
}


// Then update clean table //

// get rows from mysql
$rawRows = getMySQLRows($conn, "raw_$curStation", $numToClean);

$prevPCraw = null;
$lineNum = 0;
foreach ($rawRows as $line) {
    $curDateTime = $line["DateTime"];
    $curWatYr = wtr_yr($curDateTime, 10); // calc wat yr

    $cleanRow = array(
        "DateTime" => $line["DateTime"],
        "WatYr" =>  $curWatYr,
        "Air_Temp" => $line["Air_Temp"],
        "Rh" => $line["RH"],
        "Wind_Speed" => $line["Wind_Speed"] * 3.6,
        "Wind_Dir" => $line["Wind_Dir"],
        "Pk_Wind_Speed" => $line["Gust_Speed"] * 3.6,
        "PP_Tipper" => $line["Rain"],
        "Snow_Depth" => (2.75 - $line["Snow_Depth"]) * 100, // distance to ground is 2.75 m 
        "Solar_Rad" => $line["Solar_Rad"],
        "Batt" => $line["Batt_V"]
    );

    if (count($cleanRow) > 0) {
        $fields = implode(", ", array_keys($cleanRow));
        $values = implode("','", array_values($cleanRow));
    }

    $query = "UPDATE `clean_$curStation` SET WatYr = $curWatYr WHERE DateTime = '$curDateTime'";
    //$query = "INSERT IGNORE into `clean_$curStation` ($fields) values('$values')";


    if (!mysqli_query($conn, $query)) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }
}

mysqli_close($conn);
?>