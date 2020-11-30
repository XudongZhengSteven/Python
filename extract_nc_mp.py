# code: utf-8
# author: "Xudong Zheng" 
# email: Z786909151@163.com
# extract variable(given region by coord) from .nc4 file
import numpy as np
from netCDF4 import Dataset
import os
import pandas as pd
from pathos.multiprocessing import ProcessingPool as Pool
import re
import time


def extract_nc(path, coord_path, variable_name, precision=3, num_pool=4):
    """extract variable(given region by coord) from .nc file
    input:
        path: path of the source nc file
        coord_path: path of the coord extracted by fishnet: OID_, lon, lat
        variable_name: name of the variable need to read
        precision: the minimum precision of lat/lon, to match the lat/lon of source nc file
        num_pool: the number of processes

    output:
        {variable_name}.txt [i, j]: i(file number) j(grid point number)
        lat_index.txt/lon_index.txt
        coord.txt
    """
    print(f"variable:{variable_name}")
    coord = pd.read_csv(coord_path, sep=",")  # read coord(extract by fishnet)
    print(f"grid point number:{len(coord)}")
    coord = coord.round(precision)  # coord precision correlating with .nc file lat/lon
    result = [path + "/" + d for d in os.listdir(path) if d[-4:] == ".nc4"]
    print(f"file number:{len(result)}")
    variable = np.zeros((len(result), len(coord) + 1))  # save the path correlated with read order

    # calculate the index of lat/lon in coord from source nc file
    f1 = Dataset(result[0], 'r')
    Dataset.set_auto_mask(f1, False)
    lat_index = []
    lon_index = []
    lat = f1.variables["lat"][:]
    lon = f1.variables["lon"][:]
    for j in range(len(coord)):
        lat_index.append(np.where(lat == coord["lat"][j])[0][0])
        lon_index.append(np.where(lon == coord["lon"][j])[0][0])
    f1.close()

    # read variable based on the lat_index/lon_index, based on multiprocessing
    def read(i):
        """read variable from nc file(i), used in pool"""
        vb = []
        f = Dataset(result[i], 'r')
        vb.append(float(re.search(r"\d{6}", result[i])[0]))
        # re: the number depend on the nc file name(daily=8, month=6)
        Dataset.set_auto_mask(f, False)
        for j in range(len(coord)):
            vb.append(f.variables[variable_name][0, lat_index[j], lon_index[j]])
            # require: nc file only have three dimension
            # f.variables['Rainf_f_tavg'][0, lat_index_lp, lon_index_lp]is a mistake, we only need the file
            # that lat/lon corssed (1057) rather than meshgrid(lat, lon) (1057*1057)
        print(f"complete read file:{i}")
        return vb

    po = Pool(num_pool)  # pool
    res_po = [po.amap(read, (i,)) for i in range(len(result))]  # the results of every process
    po.close()
    po.join()
    for i in range(len(result)):
        variable[i, :] = res_po[i].get()[0]  # get varibale from result
    # sort by time
    variable = variable[variable[:, 0].argsort()]
    # save
    np.savetxt(f'{variable_name}.txt', variable, delimiter=' ')
    np.savetxt('lat_index.txt', lat_index, delimiter=' ')
    np.savetxt('lon_index.txt', lon_index, delimiter=' ')
    coord.to_csv("coord.txt")


def overview(path):
    # overview of the nc file
    result = [path + "/" + d for d in os.listdir(path) if d[-4:] == ".nc4"]
    rootgrp = Dataset(result[0], "r")
    print('****************************')
    print(f"number of nc file:{len(result)}")
    print('****************************')
    print(f"variable key:{rootgrp.variables.keys()}")
    print('****************************')
    print(f"rootgrp:{rootgrp}")
    print('****************************')
    print(f"lat:{rootgrp.variables['lat'][:]}")
    print('****************************')
    print(f"lon:{rootgrp.variables['lon'][:]}")
    print(f"variable:{rootgrp.variables}")
    print('****************************')
    variable_name = input("variable name:")  # if you want to see the variable, input its name here
    while variable_name != "":
        print('****************************')
        print(f"variable:{rootgrp.variables[variable_name]}")
        variable_name = input("variable name:")  # if you want to quit, input enter here
    rootgrp.close()


if __name__ == "__main__":
    """example"""
    start = time.time()
    path = "F:/Yanxiang/Python/gldas"
    coord_path = "F:/Yanxiang/Python/coord.txt"
    extract_nc(path, coord_path, "Snowf_tavg", precision=3)
    end = time.time()
    print("extract_nc_mp time：", end - start)
    # """Execute  code, extract variable from GLDAS nc file"""
    # path = "D:\GLADS\daily_data"
    # coord_path = "H:\GIS\Flash_drought\coord.txt"
    # coord = pd.read_csv(coord_path, sep=",")
    # overview(path)
    # extract_nc(path, coord_path, 'SoilMoist_RZ_tavg', precision=3, num_pool=8)
