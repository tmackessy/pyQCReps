#!/usr/bin/env python

# Module for interacting with CO-OPS internet resources
# Trevor Mackessy-Lloyd

import collections
import json
import os
import re
from datetime import date
from io import StringIO

import numpy as np

import pycurl
try:  # Python 3
    from io import BytesIO
except ImportError:  # Python 2
    from io import StringIO as BytesIO  # lint:ok

from osgeo import gdal, ogr, osr
gdal.UseExceptions()

# use of pycurl inferred from module documentation at:
# pycurl.sourceforge.net/doc/quickstart.html

# use of CO-OPS API for Data Retrieval per tidesandcurrents.noaa.gov/api/


class Report:
    """Retrieve, parse and convert various quality control reports to
    spatial data formats.
    Keyword arguments:
        r_type  : type of report, default: 'invalid', accepts:
                      'invalid'       : Invalid Error Report
                      'qc_check'      : Data Quality Control Report
                      'data_source'   : Data Source Report
                      'qc_flat'       : Erroneous Flat Flag Report
        day     : day of month, default: today
        stnlist : list of station ID's in report
        debug   : override auto-execution of self.get_report(),
                  self.parse_report(), and self.list_stations() by .__init__
                  default: 'n'
    Methods:
        get_report()        : retrieve report from ftp server
        parse_report()      : extract data from report
        list_stations()     : list station ID''s in report
        create_shapefile()  : generate shapefile from report
    Attributes:
        body        : raw response from ftp server
        encoding    : encoding of body, default: iso-8859-1
        data        : parsed data, stored in np array
    """
    def __init__(self, r_type='invalid', day=date.today().day,
                       stnlist=[], debug='n'):
        self.r_type = r_type
        self.day = day
        self.stnlist = stnlist
        self.debug = debug
        if self.debug == 'n':
            self.get_report()
            self.parse_report()
            self.list_stations()

    def __str__(self):
        return self.body.getvalue().decode(self.encoding)

    def __repr__(self):
        try:
            return self.data
        except:
            return self.__str__

    def get_report(self):
        """Get reports from FTP site. Calls get_url as a subfunction
        after formatting the URL. Returns the raw report and encoding.
        """
        # Currently handles:
            # Data Source Report,
            # Invalid Error Report,
            # Data Quality Control Report,
            # Erroneous Flat Flag Report,
        # To do:
            # others?
        if 0 < self.day <= 31:
            if self.day < 10:
                self.day = '0' + str(self.day)
            else:
                self.day = str(self.day)
        else:
            raise NameError('Invalid day of month specified!')
        base_url = 'ftp://tidepool.nos.noaa.gov/pub/outgoing/reports/' + \
                   self.day
        if self.r_type == 'invalid':
            url = base_url + '/INVALID_ERROR_REPORT.1105'
            print(('Retrieving Invalid Error Report from %s.' % url))
            [self.body, self.encoding] = get_url(url)
            return self
        elif self.r_type == 'qc_check':
            url = base_url + '/QC_CHECK.1040'
            print(('Retrieving Data Quality Control Report from %s.' % url))
            [self.body, self.encoding] = get_url(url)
            return self
        elif self.r_type == 'qc_flat':
            url = base_url + '/QC_FLAT.1040'
            print(('Retrieving Erroneous Flat Flag Report from %s.' % url))
            [self.body, self.encoding] = get_url(url)
            return self
        elif self.r_type == 'data_source':
            url = base_url + '/DAILY_DATA_STATUS.1040'
            print(('Retrieving Daily Data Source Report from %s.' % url))
            [self.body, self.encoding] = get_url(url)
            return self
        else:
            raise NameError('Invalid report type specified!')

    def parse_report(self):
        """Parse data from report and repack as a numpy array. Header
        and footer lengths are hard-coded. Each report type has a
        different parser.
        Currently handles:
            Invalid Error Report
            Data Quality Control Report
        To do:
            Data Source Report
            Erroneous Flat Flag Report
        """
        tmp = self.__str__()
        if self.r_type == 'invalid':
            self.data = np.genfromtxt(
                StringIO(tmp),
                skip_header=7,
                skip_footer=2,
                autostrip=True,
                dtype=('|S7', '|S1', '|S2', int),
                names='Station, DCP, Sensor, Received')
            return self.data
        elif self.r_type == 'qc_check':
            self.data = np.genfromtxt(
                StringIO(tmp),
                skip_header=13,
                skip_footer=28,
                autostrip=True,
                delimiter=[8, 2, 3, 9, 7, 6, 6, 7, 7, 7, 8, 8],
                dtype=('|S7', '|S1', '|S2', int, float, int, int, int, int,
                       int, int, int),
                names='Station, DCP, Sensor, Data_Received, ' +
                      'Percent_Data, Flat, RofC, Temp, Height, ' +
                      'Exceed_Limits, Prim/Backup, Prim/Predict')
            return self.data
    #    elif report_type == 'qc_flat':
    #        return self.data
    #    elif report_type == 'data_source':
    #        return self.data
        else:
            raise NameError('Report type not supported!')
        '''TODO: scan through data and remove all 999???? stations from
                 list prior to processing.'''
        '''
        for i in range(len(self.data)):
            if self.data[i][0]:
                #stuff
        '''

    def list_stations(self):
        """Extract a list of station ID's from self.data and store it
        separately.
        """
        for i in range(len(self.data)):
            self.stnlist.append(Station(self.data[i][0]))
        return self

    def create_shapefile(self):
        """Create shapefile at current working directory. Fields are switched
        on r_type.
        """
        # TODO: Run through this function and try to break it up.
        driver = ogr.GetDriverByName('ESRI Shapefile')

        # create the data source, i.e. the shapefile
        path = os.getcwd()
        if os.path.exists(path + '/' + '%s.shp' % self.r_type):
            driver.DeleteDataSource(path + '/' + '%s.shp' % self.r_type)
        data_source = driver.CreateDataSource(path + '/' + '%s.shp'
            % self.r_type)

        # create the spatial reference: WGS84, EPSG #4326
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        # create the layer
        layer = data_source.CreateLayer(self.r_type, srs, ogr.wkbPoint)

        # Add the fields we're interested in
        #  Station ID
        field_name = ogr.FieldDefn('Stn_ID', ogr.OFTString)
        field_name.SetWidth(7)
        layer.CreateField(field_name)
        #  Station Name
        field_name = ogr.FieldDefn('Stn_Name', ogr.OFTString)
        #field_name.SetWidth(30)  # <- this seems to be optional
        layer.CreateField(field_name)
        #  DCP
        field_name = ogr.FieldDefn('DCP', ogr.OFTString)
        field_name.SetWidth(1)
        layer.CreateField(field_name)
        #  Sensor ID
        field_name = ogr.FieldDefn('Sensor_ID', ogr.OFTString)
        field_name.SetWidth(2)
        layer.CreateField(field_name)
        if self.r_type == 'invalid':
            #  Invalid_Received
            layer.CreateField(ogr.FieldDefn('Inv_Recd', ogr.OFTInteger))
        elif self.r_type == 'qc_check':
            layer.CreateField(ogr.FieldDefn('Dat_Recd', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Pct_Recd', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Flat', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('RofC', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Temp', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Height', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Excd_Lim', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Prim/Bkp', ogr.OFTInteger))
            layer.CreateField(ogr.FieldDefn('Prim/Pre', ogr.OFTInteger))
        #elif self_type == 'qc_flat':
        #    layer.CreateField(ogr.FieldDefn('Inv_Recd', ogr.OFTInteger))
        #elif self_type == 'data_source':
        #    layer.CreateField(ogr.FieldDefn('Inv_Recd', ogr.OFTInteger))
        layer_defn = layer.GetLayerDefn()

        for i in range(len(self.stnlist)):
            if 'error' not in self.stnlist[i].metadata:
                feature = ogr.Feature(layer_defn)
                feature.SetFID(i)
                wkt = 'POINT(%f %f)' % (float(self.stnlist[i].metadata['lon']),
                                        float(self.stnlist[i].metadata['lat']))
                feature.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
                feature.SetField('Stn_ID', self.stnlist[i].metadata['id'])
                feature.SetField('Stn_Name', self.stnlist[i].metadata['name'])
                feature.SetField('DCP', self.data[i][1])
                feature.SetField('Sensor_ID', self.data[i][2])
                if self.r_type == 'invalid':
                    feature.SetField('Inv_Recd', int(self.data[i][3]))
                elif self.r_type == 'qc_check':
                    feature.SetField('Dat_Recd', int(self.data[i][3]))
                    feature.SetField('Pct_Recd', int(self.data[i][4]))
                    feature.SetField('Flat', int(self.data[i][5]))
                    feature.SetField('RofC', int(self.data[i][6]))
                    feature.SetField('Temp', int(self.data[i][7]))
                    feature.SetField('Height', int(self.data[i][8]))
                    feature.SetField('Excd_Lim', int(self.data[i][9]))
                    feature.SetField('Prim/Bkp', int(self.data[i][10]))
                    feature.SetField('Prim/Pre', int(self.data[i][11]))
                # Add these later:
                #elif self.r_type == 'qc_flat':
                #    feature.SetField('XXXXX', int(self.data[i][XXXXX]))
                #elif self.r_type == 'data_source':
                #    feature.SetField('XXXXX', int(self.data[i][XXXXX]))
                else:
                    raise NameError('Report type not supported!')
                layer.CreateFeature(feature)
                feature.Destroy()
            else:
                print(('Skipped station %s, no response from Datagetter.'
                    % str(self.stnlist[i])))


class Station:
    """Retrieve metadata for stations in the CO-OPS observing network. This
    class designed for use with the CO-OPS Data API.

    Currently written and tested against water level stations; extending to
    other station types should be trivial.

    Keyword arguments:
        station_id  : station ID of target station
        metadata    : retrieved metadata, stored as dict
        encoding    : expected encoding, default: iso-8859-1
    Methods:
        get_station()   : retrieve station metadata from CO-OPS API
    """

    def __init__(self, station_id, metadata={}, encoding='iso-8859-1'):
        self.station_id = station_id
        self.metadata = metadata
        self.encoding = encoding
        self.get_station()

    def __str__(self):
        return str(self.station_id)

    def __repr__(self):
        return str(self.station_id)

    '''TO DO: Extend generation of tac_query in get_station() for other product
    types. Simple switch on structure of existing station_id argument, or
    possibly new station_type argument.
    '''
    def get_station(self):
        """Get metadata from an active water level station via the
        CO-OPS API for Data Retrieval (DataGetter API). Takes a URL request and
        returns a json object.
        """
        _tac_query = 'http://tidesandcurrents.noaa.gov/api/datagetter?' + \
                     'station=' + str(self.station_id) +\
                     '&date=latest' + \
                     '&product=water_level' + \
                     '&units=metric' + \
                     '&time_zone=GMT' + \
                     '&format=json' + \
                     '&datum=stnd' + \
                     '&application=ED_pyQCReps'
        [_tac_response, self.encoding] = get_url(_tac_query)
        data = json.loads(_tac_response.getvalue(), encoding=self.encoding)
        if 'error' not in data:
            self.metadata = convert(data['metadata'])
        else:
            self.metadata = convert(data)
        return self


def get_url(url):
    """Send HTTP requests to a given URL and return response and encoding."""
    def header_function(header_line):
        header_line = header_line.decode('iso-8859-1')
        if ':' not in header_line:
            return
        name, value = header_line.split(':', 1)
        name = name.strip()
        value = value.strip()
        name = name.lower()
        headers[name] = value
    headers = {}
    response = BytesIO()
    c = pycurl.Curl()
    try:
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, response)
    except:  # preserve compatibility with older versions of pycurl
        c.reset()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, response.write)
    c.setopt(c.HEADERFUNCTION, header_function)
    c.perform()
    #print(('Status: %d' % c.getinfo(c.RESPONSE_CODE)))
    #print(('Transfer Time: %f' % c.getinfo(c.TOTAL_TIME)))
    c.close()
    encoding = None
    if 'content-type' in headers:
        content_type = headers['content-type'].lower()
        match = re.search('charset=(\S+)', content_type)
        if match:
            encoding = match.group(1)
            #print(('Decoding using %s' % encoding))
    if encoding is None:
        encoding = 'iso-8859-1'
        #print(('Assuming encoding is %s' % encoding))
    return response, encoding.encode('ascii', 'ignore')


def convert(data):
    """Convert iterables from unicode to strings. Kudos to stackoverflow
    user: RichieHindle"""
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data
