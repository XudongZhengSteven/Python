# code: utf-8
# author: "Xudong Zheng" 
# email: Z786909151@163.com
# extract variable(given region by coord) from .nc4 file
import numpy as np
from netCDF4 import Dataset
import os
import pandas as pd
import time
import re


def extract_nc(path, coord_path, variable_name, precision=3):
    """extract variable(given region by coord) from .nc file
    input:
        path: path of the source nc file
        coord_path: path of the coord extracted by fishnet: OID_, lon, lat
        variable_name: name of the variable need to read
        precision: the minimum precision of lat/lon, to match the lat/lon of source nc file

    output:
        {variable_name}.txt [i, j]: i(file number) j(grid point number)
        lat_index.txt/lon_index.txt
        coord.txt
    """
    print(f"variable:{variable_name}")
    coord = pd.read_csv(coord_path, sep=",")  # read coord(extract by fishnet)
    print(f"grid point number:{len(coord)}")
    coord = coord.round(precision)  # 处理单位以便与nc中lat lon一致
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

    # read variable based on the lat_index/lon_index
    for i in range(len(result)):
        f = Dataset(result[i], 'r')
        Dataset.set_auto_mask(f, False)
        variable[i, 0] = float(re.search(r"\d{8}", result[i])[0])
        for j in range(len(coord)):
            variable[i, j + 1] = f.variables[variable_name][0, lat_index[j], lon_index[j]]
            # require: nc file only have three dimension
            # f.variables['Rainf_f_tavg'][0, lat_index_lp, lon_index_lp]is a mistake, we only need the file
            # that lat/lon corssed (1057) rather than meshgrid(lat, lon) (1057*1057)
        print(f"complete read file:{i}")
        f.close()

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
    start = time.time()
    path = "H:/test"
    coord_path = "H:\GIS\Flash_drought\coord.txt"
    extract_nc(path, coord_path, "SoilMoist_RZ_tavg", precision=3)
    end = time.time()
    print("extract_nc time：", end - start)
