#
# fonctions/outils pour traiter les données de vent d'ERA5 et d'ERA5-Land
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
import numpy as np

# constantes
LON_MIN, LON_MAX = -96, -52
LAT_MIN, LAT_MAX = 40, 63


def fait_stat_mens_vit_vent_era5_et_era5l(source="era5-land", hauteur=10):
    """fonction qui fait les stat mensuelles de la vitesse du vent d'ERA5 et ERA5-Land

    la fonction calcule les statmens de la moyenne et du max quotidien du vent horaire

    la fonction est traite la vitesse du vent a 10m et 100m 

    On ne calcule que la vitesse du vent, pas la direction

    le calcule de la vitess du vent se fait à partir des données horaire du ua et va pour la hauteur 
    voulue
    
    Entree:

    source : string
                "era5" ou "era5-land" selon le choix voulu
                la réponse indique ou chercher les donnees et ecrire la sortie

    hauteur : int
                10 ou 100 selon la hauteur du vent a traiter

    sortie : fichier netcdf avec les statmens de la variable sfcwind
    
    """
    # lecture de ua et va
    if source == "era5":
        rep_src = "/home/biner/exec/1_projets/climato_vent_202509/data/reconstruction_NAM/ECMWF/ERA5/1hr"
    elif source == "era5-land":
        rep_src = "/home/biner/exec/1_projets/climato_vent_202509/data/reconstruction_NAM/ECMWF/ERA5-Land/1hr"

    if hauteur == 10:
        nom_ua = "uas"
        nom_va = "vas"
    elif hauteur == 100:
        nom_ua = "ua100m"
        nom_va = "va100m"
    ua = xr.open_mfdataset(os.path.join(rep_src, nom_ua, f"{nom_ua}_*"), engine="zarr")[f"{nom_ua}"]
    va = xr.open_mfdataset(os.path.join(rep_src, nom_va, f"{nom_va}_*"), engine="zarr")[f"{nom_va}"]

    # calcul du module et de la direction du vent
    (sfcwind_1hr, wind_dir)  = xc.indicators.atmos.wind_speed_from_vector(ua, va)

    # calcul des stat quotidiennes
    sfcwind = sfcwind_1hr.resample(time="D").mean()
    sfcwindmax = sfcwind_1hr.resample(time="D").max()
    sfcwindmax = sfcwindmax.rename("sfcwindmax")

    if hauteur == 100:
        sfcwind = sfcwind.rename("sfcwind100")
        sfcwindmax = sfcwindmax.rename("sfcwind100max")


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
    if source == "era5":
        rep_sortie = "/home/biner/exec/1_projets/climato_vent_202509/data/era5_statmens"
    elif source == "era5-land":   
        rep_sortie = "/home/biner/exec/1_projets/climato_vent_202509/data/era5l_statmens"

    d_encoding_def = dict(zlib=True, complevel=5, shuffle=True)

    # on ecrit un fichier par variable/annee
    l_annees = np.unique(sfcwind_sm.time.dt.year.values)
    for ds in [sfcwind_sm, sfcwindmax_sm]:
        d_encoding = {}
        for nv in ds.data_vars.keys():
            d_encoding[nv] = d_encoding_def
        for annee in tqdm(l_annees):
            dsr = ds.sel(time=str(annee))
            nom_var = nv.split("_")[0]
            f_nc = f"{nom_var}_era5_statmens_{annee}.nc"
            p_nc = os.path.join(rep_sortie, f_nc)
            print(f"ecriture du fichier {p_nc}")
            with ProgressBar():
                dsr.to_netcdf(p_nc, format="NETCDF4")



def main():
    fait_stat_mens_vit_vent_era5_et_era5l(source="era5", hauteur=100)


if __name__ == "__main__":
    main()
