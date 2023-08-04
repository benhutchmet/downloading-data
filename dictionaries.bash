# Dictionaries for the data download


historical_models=("BCC-CSM2-MR" "CanESM5" "CMCC-CM2-SR5" "EC-Earth3" "HadGEM3-GC31-MM" "IPSL-CM6A-LR" "MIROC6" "MPI-ESM1-2-HR" "MRI-ESM2-0" "NorCPM1")

data_nodes=("esgf.ceda.ac.uk" "crd-esgf-drc.ec.gc.ca" "esgf-data1.llnl.gov" "esgf1.dkrz.de")

wget_scripts_dir="/gws/nopw/j04/canari/users/benhutch/historical/wget_scripts"

data_dir="/gws/nopw/j04/canari/users/benhutch/historical/data"

openID="https://esgf-node.llnl.gov/esgf-idp/openid/benhutchins25"

username="benhutchins25"

models="BCC-CSM2-MR MPI-ESM1-2-HR CanESM5 CMCC-CM2-SR5 HadGEM3-GC31-MM EC-Earth3 MPI-ESM1-2-LR FGOALS-f3-L MIROC6 IPSL-CM6A-LR CESM1-1-CAM5-CMIP5 NorCPM1"

all_nodes=("esgf.ceda.ac.uk" "crd-esgf-drc.ec.gc.ca" "esgf-data1.llnl.gov" "esgf1.dkrz.de" "noresg.nird.sigma2.no" "esg-dn1.nsc.liu.se" "esgf-data.ucar.edu" "vesg.ipsl.upmc.fr" "esgf-data02.diasjp.net" "esg1.umr-cnrm.fr" "esg.lasg.ac.cn" "esgf.dwd.de" "esgf-data03.diasjp.net" "esgf-data.ucar.edu" "vesg.ipsl.upmc.fr" "esgf-data02.diasjp.net" "esg.lasg.ac.cn" "cmip.bcc.cma.cn" "esgf3.dkrz.de" "crd-esgf-drc.ec.gc.ca" "esgf-node2.cmcc.it" "esgf.bsc.es" "esgf-data.ucar.edu" "esg.lasg.ac.cn" "esgf-data1.llnl.gov")

nodes=($(echo "${all_nodes[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))
