<?php
// query's html file from hakai live website grabs javascript table. 
require 'config.php';
require 'functions.php';
$tbl = "eastbuxton";
$numToClean = 6;

$conn = mysqli_connect(MYSQLHOST, MYSQLUSER, MYSQLPASS, MYSQLDB);

if (mysqli_connect_errno())
  {
  echo "Failed to connect to MySQL: " . mysqli_connect_error();
  }

// set number of records to pull this is also the number that is appended
$maxRows = 1;
$file = file_get_contents('https://hecate.hakai.org/sn/1hourSamples/last4weeks-BuxtonEast.1hourSamples.html');

// code sourced from Ron's legacy DEF tables on galiano
preg_match('/data\.addRows\(\[(.*)\]\).*control = new google\.visualization\.ControlWrapper/s',$file,$matches);
// note: data is javascript with month as 0-11 , date is separated with colons so we can reconstruct it later (need to normalize string lengths)

$str=preg_replace('/new Date\(([0-9]+),([0-9]+),([0-9]+),([0-9]+),([0-9]+)\)/ms','"\1:\2:\3:\4:\5"',$matches[1]);
$rows=explode('],',$str);
$dataRaw = array();
if(count($rows)>0)
{
// grab most recent samples first, limited to max rows
	while(count($dataRaw) <= $maxRows && count($rows)>0)
	{
    	$dataRaw[]=str_replace(array('[',']'),'',array_pop($rows));
	}
}

// function from Ron's legacy DEF tables on galiano
function parse_observation_time($str)
{
    $date_str='';
    $parts=explode(':',$str);
    if(count($parts)==5)
    {
        $date_str=sprintf('%04d',$parts[0]).'-'.sprintf('%02d',((int)$parts[1]+1)).'-'.sprintf('%02d',$parts[2]).' '.sprintf('%02d',$parts[3]).':'.sprintf('%02d',$parts[4]).':00';
    }
    return $date_str;
}

// append array of wx observations to db until array is empty based on max rows
while (count($dataRaw) > 0) {
 if(count($dataRaw)>0)
    // gets first element of array
    $row_src=str_getcsv(array_pop($dataRaw));

{
        $dataRow=array(
            'DateTime'=>parse_observation_time($row_src[0]),  
            'Air_Temp'=>$row_src[1],
            'Relative_Humidity'=>$row_src[2],
            'Snow_Depth'=>$row_src[3], // in metres
            'Wind_Spd'=>$row_src[5], // in m/s
            'Wind_Dir'=>$row_src[6], // rm young is pointed north 
            'Air_Pressure'=>$row_src[7], // in hpa
            'SolarRad_Avg'=>$row_src[8],
            'SolarRad_24hr'=>$row_src[9],
            'Rain'=>$row_src[10],
            'Rain_24hr'=>$row_src[11],
            'Pcp_GaugeLvl'=>$row_src[12], // in metres
            'Pcp_GaugeTemp'=>$row_src[13],
            'EnclosureTemp'=>$row_src[14],
            'Panel_Temp'=>$row_src[15],
            'BattVolt'=>$row_src[16]
        );
//print_r($dataRow);   
}

if(count($dataRow)>0){
$fields = implode(", ", array_keys($dataRow));
$values = implode("','", array_values($dataRow));
}

$query = "insert ignore into `raw_$tbl` ($fields) values('$values');";
//print_r($query);

if (!mysqli_query($conn,$query))
            {
            exit("Insert Query Error description: " . mysqli_error($conn));
            }
}
// Then update clean table //

// get rows from mysql
$rawRows = getMySQLRows($conn, "raw_$tbl", $numToClean);

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

    $cleanRow = array(
        "DateTime" => $line["DateTime"],
        "Air_Temp" => $line["Air_Temp"],
        "Rh" => $line["Relative_Humidity"],
        "BP" => $line["Air_Pressure"],
        "Wind_Speed" => $line["Wind_Spd"],
        "Wind_Dir" => ($line["Wind_Dir"] >= 180 ? $line["Wind_Dir"] -= 180 : $line["Wind_Dir"] += 180), // rm young is backwards on tower
        "PP_Tipper" => $line["Rain"],
        "PC_Raw_Pipe" => $line["Pcp_GaugeLvl"] * 1000,
        "PP_Pipe" => $PP_Pipe,
        "Snow_Depth" => $line["Snow_Depth"], // distance to ground processed on unit
        "Solar_Rad" => $line["SolarRad_Avg"],
        "Batt" => $line["BattVolt"]
    );

    if (count($cleanRow) > 0) {
        $fields = implode(", ", array_keys($cleanRow));
        $values = implode("','", array_values($cleanRow));
    }

    // convert clean array to a string                    
    $string = implode("','", $cleanRow);

    if (!mysqli_query($conn, "INSERT IGNORE into `clean_$tbl` ($fields) values('$values')")) {
        exit("Insert Query Error description: " . mysqli_error($conn));
    }
}	
mysqli_close($conn);
?>