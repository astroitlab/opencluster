import time
import math
from datetime import date, datetime, timedelta, tzinfo


class MuserTime(object):
    """
    class for system time obtained from digital receiver
    """

    def __init__(self, year=0, month=0, day=0, hour=0, minute=0, second=0, millisecond=0, microsecond=0, nanosecond=0):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.millisecond = millisecond
        self.microsecond = microsecond
        self.nanosecond = nanosecond

        self.MJD_0 = 2400000.5
        self.MJD_JD2000 = 51544.5

    def get_time_stamp(self):
        obs_time = ('%4d-%02d-%02d %02d:%02d:%02d') % (
            self.year, self.month, self.day, self.hour, self.minute, self.second)

        # The date and time are Beijing Time of China, UTC = CST - 8
        tmp = time.strptime(obs_time, '%Y-%m-%d %H:%M:%S')
        return time.mktime(tmp)

    def get_fits_date_time(self):
        #self.muser.obs_date + "T00:00:00.0"
        return ('%4d-%02d-%02dT%02d:%02d:%02d.%03d%03d%03d' %
                (self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond, self.microsecond, self.nanosecond))

    def get_date_time(self):
        obsTIME = ('%4d-%02d-%02d %02d:%02d:%02d %03d%03d') % (
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond, self.microsecond)
        return datetime.strptime(obsTIME, "%Y-%m-%d %H:%M:%S %f")

    def get_short_string(self):
        return ('%04d-%02d-%02d %02d:%02d:%02d' % (self.year, self.month, self.day, self.hour, self.minute, self.second))

    def get_string(self):
        return ('%04d-%02d-%02d %02d:%02d:%02d.%03d%03d%03d' % (self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond, self.microsecond, self.nanosecond))

    def get_local_time(self):
        return self.year, self.month, self.day, self.hour, self.minute, self.second

    def set_with_date_time(self, dt ):
        self.year = dt.date().year
        self.month = dt.date().month
        self.day = dt.date().day
        self.hour = dt.time().hour
        self.minute = dt.time().minute
        self.second = dt.time().second
        self.millisecond = dt.time().microsecond//1000
        self.microsecond = dt.time().microsecond - self.millisecond*1000
        self.nanosecond = 0

    def get_second(self):
        return self.second

    def get_millisecond(self):
        return self.millisecond

    def get_microsecond(self):
        return self.microsecond

    def get_detail_time(self):
        return self.year, self.month, self.day, self.hour, self.minute, self.second, self.millisecond, self.microsecond, self.nanosecond

    def get_julian_date(self):
        mjd = self.gcal2jd(self.year, self.month, self.day)
        return mjd+ (self.hour + self.minute /60. + (self.second+self.millisecond/1000.+self.microsecond/1e6+self.nanosecond/1e9)/3600.)/24.

    def from_julian_date(self, mjd):
        self.year, self.month, self.day, f = self.jd2gcal(mjd)
        self.hour = int(f*24.)
        f = f*24. - self.hour
        self.minute = int(f*60.)
        f = f*60. - self.minute
        self.second = int(f*60.)
        f = f*60. - self.second
        self.millisecond = int(f*1000)
        f = f*1000. - self.millisecond
        self.microsecond = int(f*1000)
        f = f*1000. - self.microsecond
        self.nanosecond = int(f*1000)

    def fpart(self, x):
        """Return fractional part of given number."""
        return math.modf(x)[0]


    def ipart(self, x):
        """Return integer part of given number."""
        return math.modf(x)[1]


    def is_leap(self, year):
        """Leap year or not in the Gregorian calendar."""
        x = math.fmod(year, 4)
        y = math.fmod(year, 100)
        z = math.fmod(year, 400)

        # Divisible by 4 and,
        # either not divisible by 100 or divisible by 400.
        return not x and (y or not z)


    def gcal2jd(self, year, month, day):
        """Gregorian calendar date to Julian date.

        The input and output are for the proleptic Gregorian calendar,
        i.e., no consideration of historical usage of the calendar is
        made.

        Parameters
        ----------
        year : int
            Year as an integer.
        month : int
            Month as an integer.
        day : int
            Day as an integer.

        Returns
        -------
        jd1, jd2: 2-element tuple of floats
            When added together, the numbers give the Julian date for the
            given Gregorian calendar date. The first number is always
            MJD_0 i.e., 2451545.5. So the second is the MJD.

        Notes
        -----
        The returned Julian date is for mid-night of the given date. To
        find the Julian date for any time of the day, simply add time as a
        fraction of a day. For example Julian date for mid-day can be
        obtained by adding 0.5 to either the first part or the second
        part. The latter is preferable, since it will give the MJD for the
        date and time.

        BC dates should be given as -(BC - 1) where BC is the year. For
        example 1 BC == 0, 2 BC == -1, and so on.

        Negative numbers can be used for `month` and `day`. For example
        2000, -1, 1 is the same as 1999, 11, 1.

        The Julian dates are proleptic Julian dates, i.e., values are
        returned without considering if Gregorian dates are valid for the
        given date.

        The input values are truncated to integers.

        """
        year = int(year)
        month = int(month)
        day = int(day)

        a = self.ipart((month - 14) / 12.0)
        jd = self.ipart((1461 * (year + 4800 + a)) / 4.0)
        jd += self.ipart((367 * (month - 2 - 12 * a)) / 12.0)
        x = self.ipart((year + 4900 + a) / 100.0)
        jd -= self.ipart((3 * x) / 4.0)
        jd += day - 2432075.5  # was 32075; add 2400000.5

        jd -= 0.5  # 0 hours; above JD is for midday, switch to midnight.

        return jd


    def jd2gcal(self, jd2):
        """Julian date to Gregorian calendar date and time of day.

        The input and output are for the proleptic Gregorian calendar,
        i.e., no consideration of historical usage of the calendar is
        made.

        Parameters
        ----------
        jd1, jd2: int
            Sum of the two numbers is taken as the given Julian date. For
            example `jd1` can be the zero point of MJD (MJD_0) and `jd2`
            can be the MJD of the date and time. But any combination will
            work.

        Returns
        -------
        y, m, d, f : int, int, int, float
            Four element tuple containing year, month, day and the
            fractional part of the day in the Gregorian calendar. The first
            three are integers, and the last part is a float.

        Examples
        --------
        >>> jd2gcal(2400000.5, 51544.0)
        (2000, 1, 1, 0.0)

        Notes
        -----
        The last element of the tuple is the same as

           (hh + mm / 60.0 + ss / 3600.0) / 24.0

        where hh, mm, and ss are the hour, minute and second of the day.

        See Also
        --------
        gcal2jd

        """
        from math import modf

        jd1 = self.MJD_0

        jd1_f, jd1_i = modf(jd1)
        jd2_f, jd2_i = modf(jd2)

        jd_i = jd1_i + jd2_i

        f = jd1_f + jd2_f

        # Set JD to noon of the current date. Fractional part is the
        # fraction from midnight of the current date.
        if -0.5 < f < 0.5:
            f += 0.5
        elif f >= 0.5:
            jd_i += 1
            f -= 0.5
        elif f <= -0.5:
            jd_i -= 1
            f += 1.5

        l = jd_i + 68569
        n = self.ipart((4 * l) / 146097.0)
        l -= self.ipart(((146097 * n) + 3) / 4.0)
        i = self.ipart((4000 * (l + 1)) / 1461001)
        l -= self.ipart((1461 * i) / 4.0) - 31
        j = self.ipart((80 * l) / 2447.0)
        day = l - self.ipart((2447 * j) / 80.0)
        l = self.ipart(j / 11.0)
        month = j + 2 - (12 * l)
        year = 100 * (n - 49) + i + l

        return int(year), int(month), int(day), f

    def copy(self, target):
        self.year = target.year
        self.month = target.month
        self.day = target.day
        self.hour = target.hour
        self.minute = target.minute
        self.second = target.second
        self.millisecond = target.millisecond
        self.microsecond = target.microsecond
        self.nanosecond = target.nanosecond

