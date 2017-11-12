#!/bin/bash


dir=$(pwd)
LOG_FILE="$dir/acs1115_logfile.txt"
exec > >(tee -a ${LOG_FILE} )
exec 2> >(tee -a ${LOG_FILE} >&2)


starttime="start: `date +"%T"`"

mkdir acs_temp_cproj
cd acs_temp_cproj

declare -A states; states[al]=Alabama; states[ak]=Alaska; states[az]=Arizona; states[ar]=Arkansas; states[ca]=California; states[co]=Colorado; states[ct]=Connecticut; states[de]=Delaware; states[dc]=DistrictOfColumbia; states[fl]=Florida; states[ga]=Georgia; states[hi]=Hawaii; states[id]=Idaho; states[il]=Illinois; states[in]=Indiana; states[ia]=Iowa; states[ks]=Kansas; states[ky]=Kentucky; states[la]=Louisiana; states[me]=Maine; states[md]=Maryland; states[ma]=Massachusetts; states[mi]=Michigan; states[mn]=Minnesota; states[ms]=Mississippi; states[mo]=Missouri; states[mt]=Montana; states[ne]=Nebraska; states[nv]=Nevada; states[nh]=NewHampshire; states[nj]=NewJersey; states[nm]=NewMexico; states[ny]=NewYork; states[nc]=NorthCarolina; states[nd]=NorthDakota; states[oh]=Ohio; states[ok]=Oklahoma; states[or]=Oregon; states[pa]=Pennsylvania; states[pr]=PuertoRico; states[ri]=RhodeIsland; states[sc]=SouthCarolina; states[sd]=SouthDakota; states[tn]=Tennessee; states[tx]=Texas; states[us]=UnitedStates; states[ut]=Utah; states[vt]=Vermont; states[va]=Virginia; states[wa]=Washington; states[wv]=WestVirginia; states[wi]=Wisconsin; states[wy]=Wyoming;


numberargs=$#
loopstates="$@"

if [ $# -eq 0 ]
then 
loopstates=${!states[@]};
fi


for var in $loopstates
do
    echo "$var"
    echo "downloading $var all geo not tracts and bgs"
    curl --progress-bar https://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_by_state/${states[$var]}_All_Geographies_Not_Tracts_Block_Groups.zip -O
    echo "downloading $var all tracts and bgs"
    curl --progress-bar https://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_by_state/${states[$var]}_Tracts_Block_Groups_Only.zip -O
    echo "unzipping $var all geo not tracts and bgs"
    unzip -qq ${states[$var]}_All_Geographies_Not_Tracts_Block_Groups.zip -d group1
    echo "unzipping $var all tracts and bgs"
    unzip -qq ${states[$var]}_Tracts_Block_Groups_Only.zip -d group2
done


echo "creating temporary directories"
mkdir staged
mkdir combined
mkdir sorted

mkdir geostaged
mkdir geocombined
mkdir geosorted

mkdir joined

echo "processing all files: not tracts and bgs"
cd group1
for file in *20155**0***000.txt ; do mv $file ${file//.txt/a.csv} ; done
for i in *20155**0***000*.csv; do echo "writing p_$i"; while IFS=, read f1 f2 f3 f4 f5 f6; do echo "$f3$f6,"; done < $i > p_$i; done
mv p_* ../staged/

echo "processing all files: tracts and bgs"
cd ../group2
for file in *20155**0***000.txt ; do mv $file ${file//.txt/b.csv} ; done
for i in *20155**0***000*.csv; do echo "writing p_$i"; while IFS=, read f1 f2 f3 f4 f5 f6; do echo "$f3$f6,"; done < $i > p_$i; done
mv p_* ../staged/


cd ../staged
echo "combining tract and bg files with all other geographies: estimates"
for i in $(seq -f "%03g" 1 122); do cat p_e20155**0"$i"000*.csv >> eseq"$i".csv; done;
echo "combining tract and bg files with all other geographies: margin of error"
for i in $(seq -f "%03g" 1 122); do cat p_m20155**0"$i"000*.csv >> mseq"$i".csv; done;
mv *seq* ../combined/

cd ../combined

echo "replacing suppressed fields with null"
for file in *.csv; do perl -pi -e 's/\.,/,/g' $file; done;

for file in *.csv; do echo "sorting $file"; sort $file > ../sorted/$file; done;

echo "creating geography key file"
cd ../group1
for file in g20155**.csv; do mv $file ../geostaged/$file; done;

cd ../geostaged
for file in *.csv; do cat $file >> ../geocombined/geo_concat.csv; done;

cd ../geocombined
awk -F "\"*,\"*" '{print $2 $5}' geo_concat.csv > geo_key.csv
tr A-Z a-z < geo_key.csv > geo_key_lowercase.csv
paste -d , geo_key_lowercase.csv geo_concat.csv > acs_geo_2015.csv
sort acs_geo_2015.csv > ../geosorted/acs_geo_2015.csv

cd ../sorted
for file in *.csv; do echo "joining $file with geography"; join -t , -1 1 -2 1 ../geosorted/acs_geo_2015.csv ./$file > ../joined/$file; done;

echo "files joined"

cd ..

mkdir schemas

echo "downloading master column list"
curl --progress-bar https://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt -O

echo "creating schema files"

# remove first line
sed 1d ACS_5yr_Seq_Table_Number_Lookup.txt > no_header.csv

# only copy actual column entries - no metadata
# columns only have integer values in field 4
awk -F, '$4 ~ /^[0-9]+$/' no_header.csv > columns_list.csv

# create a schema file for each sequence file.  kickstart it with geography fields
n=122;for i in $(seq -f "%04g" ${n});do echo -n "KEY,FILEID,STUSAB,SUMLEVEL,COMPONENT,LOGRECNO,US,REGION,DIVISION,STATECE,STATE,COUNTY,COUSUB,PLACE,TRACT,BLKGRP,CONCIT,AIANHH,AIANHHFP,AIHHTLI,AITSCE,AITS,ANRC,CBSA,CSA,METDIV,MACC,MEMI,NECTA,CNECTA,NECTADIV,UA,BLANK1,CDCURR,SLDU,SLDL,BLANK2,BLANK3,ZCTA5,SUBMCD,SDELM,SDSEC,SDUNI,UR,PCI,BLANK4,BLANK5,PUMA5,BLANK6,GEOID,NAME,BTTR,BTBG,BLANK7" > "./schemas/schema$i.txt"; done;

# loop through master column list, add each valid column to its schema file as type float
while IFS=',' read f1 f2 f3 f4 f5; do echo -n ","`printf $f2`"_"`printf %03d $f4` >> "./schemas/schema$f3.txt"; done < columns_list.csv;

cd schemas

for file in *.csv; do sed -i -e '$a\' file; done;

echo "schema files created"

cd ../joined

# prepend each file with corresponding header - concat?
# cat schema0001.txt eseq001.csv > hmmm.txt