#
# fonctions/outils pour traiter les données de vent d'ERA5
#
# S. Biner, oct 2025
#

# importations
import os
import glob

from tqdm import tqdm
import xarray as xr
import xclim as xc
from dask.diagnostics import ProgressBar
import xscen as xs

# constantes
LON_MIN, LON_MAX = -96, -52
LAT_MIN, LAT_MAX = 40, 63


def fait_stat_mens_era5():
    """fonction qui fait les stat mensuelles des variables d'ERA5"""
    # lecture de uas et vas
    rep_era5 = "/home/biner/exec/1_projets/202509_climato_vent/data/reconstruction_NAM/ECMWF/ERA5/1hr"
    uas = xr.open_mfdataset(os.path.join(rep_era5, "uas", "uas_*"), engine="zarr").uas
    vas = xr.open_mfdataset(os.path.join(rep_era5, "vas", "vas_*"), engine="zarr").vas

    # calcul du module et de la direction du vent
    (sfcwind_1hr, wind_dir)  = xc.indicators.atmos.wind_speed_from_vector(uas, vas)

    # calcul des stat quotidiennes
    sfcwind = sfcwind_1hr.resample(time="D").mean()
    sfcwindmax = sfcwind_1hr.resample(time="D").max()
    sfcwindmax = sfcwindmax.rename("sfcwindmax")


    # calcul des stat mensuelles
    def calcule_stat_mens(da):
        da_moy = da.resample(time="MS").mean()
        da_min = da.resample(time="MS").min()
        da_max = da.resample(time="MS").max()
        da_std = da.resample(time="MS").std()
        nom_var = da.name
        # da_moy = da_moy.rename(n)
        # da_min = da_min.rename(nom_var+"_min")
        # da_max = da_max.rename(nom_var+"_max")
        # da_std = da_std.rename(nom_var+"_std")
        ds = xr.Dataset()
        ds[nom_var+"_moy"] = da_moy
        ds[nom_var+"_min"] = da_min
        ds[nom_var+"_max"] = da_max
        ds[nom_var+"_std"] = da_std
        return ds

    sfcwind_sm = calcule_stat_mens(sfcwind)
    sfcwindmax_sm = calcule_stat_mens(sfcwindmax)

    # ecriture du fichier de sortie
    rep_sortie = "/home/biner/exec/1_projets/202509_climato_vent/data/era5_statmens"
    d_encoding_def = dict(zlib=True, complevel=5, shuffle=True)
    for ds in [sfcwind_sm, sfcwindmax_sm]:
        d_encoding = {}
        for nv in ds.data_vars.keys():
            d_encoding[nv] = d_encoding_def
        nom_var = nv.split("_")[0]
        f_nc = f"{nom_var}_era5_statmens.nc"
        p_nc = os.path.join(rep_sortie, f_nc)
        print(f"ecriture du fichier {p_nc}")
        with ProgressBar():
            ds.to_netcdf(p_nc, format="NETCDF4")







def main():
    fait_stat_mens_era5l()


if __name__ == "__main__":
    main()




