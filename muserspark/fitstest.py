from astropy.io import fits
hdulist = fits.open('E:/astrodata/current2Kx2K_l.fts')
hdulist.info()
# print len(hdulist)

dd = hdulist[0].header

# print len(dd)
# for k,v in dd.items():
#     print k,v

hdulist.close()

from astropy.utils.data import download_file
fits_file = download_file('http://data.astropy.org/tutorials/FITS-Header/input_file.fits',
                          cache=True)
fits.info(fits_file)
print("Before modifications:")
print()
print("Extension 0:")
print(repr(fits.getheader(fits_file, 0)))
print()
print("Extension 1:")
print(repr(fits.getheader(fits_file, 1)))