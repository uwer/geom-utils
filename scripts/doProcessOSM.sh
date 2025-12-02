#!/bin/bash


#flist=`ls -1 *.pbf`


SHP_POSTFIX="-latest-free.shp.zip"
PBF_POSTFIX="-latest.osm.pbf"

function unzipRelative()
{
        echo "unzipRelative dir $1"
        list=`ls -1 $1/*$SHP_POSTFIX`
        #echo "$list"

        for l in $list ; do
          lf=$(basename $l)
          echo "bname $lf"
          prefix=${lf%%$SHP_POSTFIX*}
          mkdir $prefix > /dev/null
          cd $prefix
          unzip $l
          cd ..
        done

}



function createGPKG()
{
        echo "createGPKG dir $1"
        list=`ls -1 $1/*$PBF_POSTFIX`
        #echo "$list"

        for l in $list ; do
          lf=$(basename $l)
          echo "bname $lf"
          prefix=${lf%%$PBF_POSTFIX*}
          mkdir $prefix > /dev/null
          cd $prefix
          ogr2ogr -f GPKG "${prefix}-OSM.gpkg" $l points
          ogr2ogr -f GPKG -append "${prefix}-OSM.gpkg" $l lines
          cd ..
          
        done

}

function moveCreated()
{
        echo "move dirs $1 to $2"
        list=`ls -d1 $1/*/`
        #echo "$list"

        for l in $list ; do
        	if [ -e "$l/gis_osm_traffic_a_free_1.shp" ] ; then
	          lf=$(basename $l)
	          echo "bname $lf"
	          prefix=${lf%%$PBF_POSTFIX*}
	          mv $1/$prefix $2
            fi
        done
}


function move()
{
        echo "move dir $1 as $2"
        plist=`ls -1 $1/*$2`
        #echo "$plist"

        for l in $plist ; do
          lf=$(basename $l)
          echo "bname $lf"
          prefix=${lf%%$2*}

          mv $l  ${prefix}/




        done

}

command=$1
$command `pwd` ${@:2} 



