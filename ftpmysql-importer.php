<?php
// This script updates the viu hydromet mysql data base tables for the FTS sites
require 'fts_ftp_config.php';
require 'config.php';
require 'functions.php';

$ftpFilename = "ftp://".FTPUSER.":".FTPPASS."@".FTPHOST;

$conn = mysqli_connect(MYSQLHOST, MYSQLUSER, MYSQLPASS, MYSQLDB);

$numRowsToClean = 581;

if (mysqli_connect_errno()) {
    echo "Failed to connect to MySQL: " . mysqli_connect_error();
}

if (!file_exists($ftpFilename)) {
        echo "File not found. Make sure you specified the correct path.\n";
        exit;
}

// create array from csv file
$csv = array_map('str_getcsv', file($ftpFilename));

// store fields for mysql query
$fields = implode(",",array_slice($csv[0], 1));
$lines = 0;

//loop through csv and insert to current station
foreach ($csv as $line) {
        if($lines == 0){ 
			$lines++;
			continue; 
        }
        
        $lines++;
 
        # grab current station name without white space to match mysql tbl
        $curStation = strtolower(str_replace(' ', '', $line[0]));

        // remove station name from array bc it is not in mysql tbl
        $data = array_slice($line, 1);

        // format datetime to mysql time format, time is PST
        $data[0] = date("Y-m-d H:i:s", strtotime($data[0])); 

        $linemysql = implode("','",$data);

        // use the first entry of the linearray array to find the appropriate table. 
         $query = "insert ignore into `raw_$curStation` ($fields) values('$linemysql');";

    if (!mysqli_query($conn, $query)) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }        
        
}

// Start Clean Table Update

$stations = array(
    "apelake",
    "claytonfalls",
    "homathko",
    "klinaklini",
    "lowercain",
    "machmell",
    "machmellkliniklini",
    "mountarrowsmith",
    "mountcayley",
    "perseverance",
    "tetrahedron",
    "plummerhut"
);

// Define array map of station precip location
$pcpMap = array(
    "apelake" => "Pcp_raw",
    "claytonfalls" => "Pcp_raw",
    "homathko" => "PC",
    "klinaklini" => "PC",
    "lowercain" => "PC",
    "machmell" => "Pcp_raw",
    "machmellkliniklini" => "Pcp_raw",
    "mountarrowsmith" => "PC",
    "mountcayley" => "PC", // no pipe here but need to define
    "perseverance" => "PC",
    "tetrahedron" => "PC",
    "plummerhut" => "PC" // no pipe here
);

// map for different SWE col names
$swMap = array(
    "apelake" => "SW_SSG",
    "claytonfalls" => "SW_SSG",
    "homathko" => "SW",
    "klinaklini" => "SW",
    "lowercain" => "SW",
    "machmell" => "SW_SSG", // no scale here but need to define
    "machmellkliniklini" => "SW_SSG",
    "mountarrowsmith" => "SW",
    "mountcayley" => "SW", // no scale here but need to define
    "perseverance" => "SW",
    "tetrahedron" => "SW_SSG",
    "plummerhut" => "SW" // no scale here but need to define
);

// define array of offsets to apply to stations
$gdist = array(
    #"claytonfalls" => 506.3,
    #"homathko" => 610.0,
    #"klinaklini" => 565.9,
    "plummerhut" => 630 // need to define from cur. conditions file that is on usb still in the data logger
);

$fields = "DateTime, Air_Temp, RH, BP, Wind_Speed, Wind_Dir, Pk_Wind_Speed, Pk_Wind_Dir, PP_Tipper, PC_Raw_Pipe, PP_Pipe, Snow_Depth, SWE, Solar_Rad, Soil_Moisture, Batt";

foreach ($stations as $curStation) {
    //select from bottom of table and skip the first row of the query defined in the first line under the calcs section
    $rawRows = getMySQLRows($conn, "raw_$curStation", $numRowsToClean);

    // grab key for pcp value
    $pcpKey = $pcpMap[$curStation];
    $swKey = $swMap[$curStation];
    $prevPCraw = null;
    $lineNum = 0;
    foreach ($rawRows as $line) {

        ///// calcs //////
        if ($lineNum == 0) {
            //$PP_Pipe = 0;
            $prevPCraw = $line[$pcpKey];
            $lineNum++;
            // need to skip  line on first row so PP_pipe not set to 0
            continue;
        } else {
            $PP_Pipe = $line[$pcpKey] - $prevPCraw;
        }

        // store current pc_raw val for next row    
        $prevPCraw = $line[$pcpKey];
        $lineNum++;

        // convert air pressure 
        $kpa = ($line['BP']) / 10;

        // apply snow depth offsets to stations that need it 
        if (array_key_exists($curStation, $gdist)) {
            $snowDepth = $line['SDepth'] + $gdist[$curStation];
        } else {
            $snowDepth = $line['SDepth'];
        }

        //// Create new clean row for clean tbl //// 
        $row_select = array(
            $line['DateTime'],
            $line['Temp'],
            $line['Rh'],
            $kpa,
            $line['Wspd'],
            $line['Dir_Raw_'],
            $line['Mx_Spd'],
            $line['Mx_Dir'],
            $line['Rn_1_Raw_'],
            $line[$pcpKey],
            $PP_Pipe,
            $snowDepth,
            $line[$swKey],
            $line['PYR'],
            $line['SM_Raw_'],
            $line['Vtx']
        );

        // convert clean array to a string                    
        $string = implode("','", $row_select);

        // import to clean tbl 
        if (!mysqli_query($conn, "INSERT IGNORE into `clean_$curStation` ($fields) values('$string')")) {
            exit("Insert Query Error description: " . mysqli_error($conn));
        }
    }
}

// free result set   
mysqli_close($conn);

?>