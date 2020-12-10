<?php

function getMySQLRows($conn, $stationName, $numRows) {

    $sql = "(SELECT * FROM `$stationName` ORDER BY DateTime desc LIMIT $numRows) order by DateTime";

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

// return water year based on input date time string
function wtr_yr ($DATETIME, $START_MONTH=10) {
  # Convert dates into POSIXlt
  $datetime = strtotime($DATETIME);
  $curYear = date("Y", $datetime);
  $curMonth= date("m",$datetime);
  # Year offset
  if($curMonth >= $START_MONTH){
    $offset = 1;
  }
  else{$offset = 0;}

  # water year
  $adjYear = $curYear+$offset;

  # return water year
  return $adjYear;
} 

?>