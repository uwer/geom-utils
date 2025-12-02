#!/bin/bash

###
# Collection of open street map data collections, there are the general PBF datasets as well as the specialised context data sets as shp  files
#  
##


SHP_POSTFIX="-latest-free.shp.zip"
PBF_POSTFIX="-latest.osm.pbf"
POSTFIX=$PBF_POSTFIX



#wget ${PREFIX}africa${POSTFIX}
#wget ${PREFIX}antarctica${POSTFIX}
#wget ${PREFIX}asia${POSTFIX}
#wget ${PREFIX}australia-oceania${POSTFIX}
#wget ${PREFIX}central-america${POSTFIX}
#wget ${PREFIX}europe${POSTFIX}
#wget ${PREFIX}north-america${POSTFIX}
#wget ${PREFIX}south-america${POSTFIX}


##australia-oceania sub regions   - shape files only 
POSTFIX=$SHP_POSTFIX
PREFIX="http://download.geofabrik.de/australia-oceania/"
mkdir oceania > /dev/null
cd oceania

wget ${PREFIX}australia${POSTFIX}

POSTFIX=$SHP_POSTFIX

#wget ${PREFIX}american-oceania${POSTFIX}
wget ${PREFIX}australia${POSTFIX}
wget ${PREFIX}cook-islands${POSTFIX}
wget ${PREFIX}fiji${POSTFIX}
wget ${PREFIX}ile-de -lipperton${POSTFIX}
wget ${PREFIX}kiribati${POSTFIX}
wget ${PREFIX}marshall-islands${POSTFIX}
wget ${PREFIX}micronesia${POSTFIX}
wget ${PREFIX}nauru${POSTFIX}
wget ${PREFIX}new-caledonia${POSTFIX}
wget ${PREFIX}new-zealand${POSTFIX}
wget ${PREFIX}niue${POSTFIX}
wget ${PREFIX}palau${POSTFIX}
wget ${PREFIX}papua-new-guinea${POSTFIX}
wget ${PREFIX}pitcairn-islands${POSTFIX}
wget ${PREFIX}polynesie-francaise${POSTFIX}
wget ${PREFIX}samoa${POSTFIX}
wget ${PREFIX}solomon-islands${POSTFIX}
wget ${PREFIX}tokelau${POSTFIX}
wget ${PREFIX}tonga${POSTFIX}
wget ${PREFIX}tuvalu${POSTFIX}
wget ${PREFIX}vanuatu${POSTFIX}
wget ${PREFIX}wallis-et-futuna${POSTFIX}




# asia general
POSTFIX=$SHP_POSTFIX
PREFIX="https://download.geofabrik.de/asia/"
cd ..
mkdir asia > /dev/null
cd asia

#wget ${PREFIX}afghanistan${POSTFIX}
#wget ${PREFIX}armenia${POSTFIX}
#wget ${PREFIX}azerbaijan${POSTFIX}
wget ${PREFIX}bangladesh${POSTFIX}
#wget ${PREFIX}bhutan${POSTFIX}
wget ${PREFIX}cambodia${POSTFIX}
#wget ${PREFIX}china${POSTFIX}
wget ${PREFIX}east-timor${POSTFIX}
wget ${PREFIX}gcc-states${POSTFIX}
#wget ${PREFIX}iran${POSTFIX}
#wget ${PREFIX}iraq${POSTFIX}
#wget ${PREFIX}israel-and-palestine${POSTFIX}
#wget ${PREFIX}jordan${POSTFIX}
#wget ${PREFIX}Kazakhstan${POSTFIX}
#wget ${PREFIX}Kyrgyzstan${POSTFIX}
wget ${PREFIX}laos${POSTFIX}
#wget ${PREFIX}lebanon${POSTFIX}
wget ${PREFIX}malaysia-singapore-brunei${POSTFIX}
wget ${PREFIX}maldives${POSTFIX}
#wget ${PREFIX}mongolia${POSTFIX}
wget ${PREFIX}myanmar${POSTFIX}
wget ${PREFIX}nepal${POSTFIX}
#wget ${PREFIX}north-korea${POSTFIX}
wget ${PREFIX}pakistan${POSTFIX}
wget ${PREFIX}philippines${POSTFIX}
#wget ${PREFIX}russian-federation${POSTFIX}
wget ${PREFIX}south-korea${POSTFIX}
wget ${PREFIX}sri-lanka${POSTFIX}
#wget ${PREFIX}syria${POSTFIX}
wget ${PREFIX}taiwan${POSTFIX}
#wget ${PREFIX}tajikistan${POSTFIX}
wget ${PREFIX}thailand${POSTFIX}
#wget ${PREFIX}turkmenistan${POSTFIX}
#wget ${PREFIX}uzbekistan${POSTFIX}
wget ${PREFIX}vietnam${POSTFIX}
#wget ${PREFIX}yemen${POSTFIX}
unzip *.zip

POSTFIX=$PBF_POSTFIX

#wget ${PREFIX}afghanistan${POSTFIX}
#wget ${PREFIX}armenia${POSTFIX}
#wget ${PREFIX}azerbaijan${POSTFIX}
wget ${PREFIX}bangladesh${POSTFIX}
#wget ${PREFIX}bhutan${POSTFIX}
wget ${PREFIX}cambodia${POSTFIX}
#wget ${PREFIX}china${POSTFIX}
wget ${PREFIX}east-timor${POSTFIX}
wget ${PREFIX}gcc-states${POSTFIX}
#wget ${PREFIX}iran${POSTFIX}
#wget ${PREFIX}iraq${POSTFIX}
#wget ${PREFIX}israel-and-palestine${POSTFIX}
#wget ${PREFIX}jordan${POSTFIX}
#wget ${PREFIX}Kazakhstan${POSTFIX}
#wget ${PREFIX}Kyrgyzstan${POSTFIX}
wget ${PREFIX}laos${POSTFIX}
#wget ${PREFIX}lebanon${POSTFIX}
wget ${PREFIX}malaysia-singapore-brunei${POSTFIX}
wget ${PREFIX}maldives${POSTFIX}
#wget ${PREFIX}mongolia${POSTFIX}
wget ${PREFIX}myanmar${POSTFIX}
wget ${PREFIX}nepal${POSTFIX}
#wget ${PREFIX}north-korea${POSTFIX}
wget ${PREFIX}pakistan${POSTFIX}
wget ${PREFIX}philippines${POSTFIX}
#wget ${PREFIX}russian-federation${POSTFIX}
wget ${PREFIX}south-korea${POSTFIX}
wget ${PREFIX}sri-lanka${POSTFIX}
#wget ${PREFIX}syria${POSTFIX}
wget ${PREFIX}taiwan${POSTFIX}
#wget ${PREFIX}tajikistan${POSTFIX}
wget ${PREFIX}thailand${POSTFIX}
#wget ${PREFIX}turkmenistan${POSTFIX}
#wget ${PREFIX}uzbekistan${POSTFIX}
wget ${PREFIX}vietnam${POSTFIX}
#wget ${PREFIX}yemen${POSTFIX}



# Indonesia sub section
# get indnesia total pbf 
mkdir indonesia > /dev/null
cd indonesia

wget ${PREFIX}indonesia${POSTFIX}

POSTFIX=$SHP_POSTFIX
PREFIX="https://download.geofabrik.de/asia/indonesia/"

wget ${PREFIX}java${POSTFIX}
wget ${PREFIX}kalimantan${POSTFIX}
wget ${PREFIX}kaluku${POSTFIX}
wget ${PREFIX}nusa-tenggara${POSTFIX}
wget ${PREFIX}papua${POSTFIX}
wget ${PREFIX}sulawesi${POSTFIX}
wget ${PREFIX}sumatra${POSTFIX}




