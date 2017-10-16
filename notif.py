from urllib.request import urlopen
#from datetime import datetime
import datetime
import sqlite3
import os.path


# Field Data                                    Required    Format  Length      Content
# 1     Material ID                             *           AN      4           VK01
# 2     Record ID                               *           N       3           001
# 3     Quoting date                            *           N       8           CCYYMMDD
# 4     Quoting time                            *           N       6           HHMMSS
# 5     Exchange rate group                     *           N       4           0001 = list
#                                                                               0002 = valuation
# 6     Currency ISO code                       *           AN      3
# 7     Counter currency ISO code               *           AN      3           EUR = euro rates
# 8     Middle rate for commercial transactions             N       13          6 int. + 7 dec.
# 9     Buying rate for commercial transactions             N       13          6 int. + 7 dec.
# 10    Sales rate for commercial transactions              N       13          6 int. + 7 dec.
# 11    Cash rate for buying                                N       13          6 int. + 7 dec.
# 12    Cash rate for selling                               N       13          6 int. + 7 dec.
# 13    Direction of change from previous value             AN      1           +, - or empty
# 14    Currency convertibility                 *           AN      1           K = convertible
#                                                                               E = non- convertible
# 15    Currency euro data                      *           N       1           1 = euro currency
#                                                                               0 = not a euro currency
# 16    Euro adoption date                                  N       8           CCYYMMDD
# 17    Currency expiry                         *           AN      1           K = in use
#                                                                               E = not in use
#
# 4 3 8 6 4 3 3 13 13 13 13 13 1 1 1 8 1
# 4 7 15 21 25 28 31 44 57 70 83 96 97 98 99 107 108


def beautify(datarows):
    # Päivämäärä kohdalleen
    for datarow in datarows:
        datarow[2] = datetime.datetime.strptime(datarow[2] + " " + datarow[3], '%Y%m%d %H%M%S')

    # Desimaalit kohdalleen
    for n in range(7, 12):
        for datarow in datarows:
            temp_kokonaisluvut = datarow[n][0:6]
            temp_desimaalit = datarow[n][6:13]
            datarow[n] = float(temp_kokonaisluvut + "." + temp_desimaalit)

    # Poistetaan turha aika
    # for datarow in datarows:
    #     del datarow[3]

    return datarows


class CurrencyData:
    # Return structure:
    #
    # [0]	timecode
    # [1]	cur
    # [2]	cur
    # [3]	middle
    # [4]	buying
    # [5]	selling

    cutpoints = [0, 4, 7, 15, 21, 25, 28, 31, 44, 57, 70, 83, 96, 97, 98, 99, 107, 108]

    def __init__(self, address):
        self.rows = None
        self.datarows = None
        self.datarow_row = None
        self.web_file_contents = urlopen(address).read(15000)
        self.web_file_contents = self.web_file_contents.decode('utf8').split("\n")

    def get_currency_pair_data(self, pair):
        self.rows = []
        self.datarows = []
        self.datarow_row = []
        for n, row in enumerate(self.web_file_contents):
            if pair in row:
                self.rows.append(n)
            else:
                pass

        print("found {} lines".format(len(self.rows)))

        # get stuff by characters from given rows
        for row in self.rows:
            for n in range(len(self.cutpoints) - 1):
                cutpoint_a = self.cutpoints[n]
                cutpoint_b = self.cutpoints[n + 1]
                cutting_temp = self.web_file_contents[row][cutpoint_a:cutpoint_b]
                self.datarow_row.append(cutting_temp)

            self.datarows.append(self.datarow_row)
            self.datarow_row = []

        self.datarows = beautify(self.datarows)

        return self.datarows


class Databasehandler:
    # Database structure:
    #
    # [0]	date 			as datetime
    # [1]	p1 (middle) 	as real
    # [2]	p2 (buying) 	as real
    # [3]	p3 (selling) 	as real

    def __init__(self, database_file):
        self.all = None
        self.data_inserted = None
        self.all_dates = []

        if os.path.exists(database_file):
            self.database_exists = True
        else:
            self.database_exists = False

        self.conn = sqlite3.connect(database_file, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.conn.cursor()

        # Create table
        if not self.database_exists:
            self.c.execute("CREATE TABLE kurssit(date timestamp, price1 real, price2 real, price3 real)")
            self.conn.commit()
        else:
            print("Database file (", database_file, ") already exists.")

    def push_to_database(self, pdate, p1, p2, p3):
        self.data_inserted = (pdate, p1, p2, p3)
        self.c.execute("INSERT INTO kurssit VALUES (?, ?, ? ,?)", self.data_inserted)
        self.conn.commit()
        return self.c.lastrowid

    def get_all_dates(self):
        for item in self.all:
            self.all_dates.append(item[0])
        return self.all_dates

    def get_all_from_database(self):
        self.c.execute("SELECT * FROM kurssit")
        self.all = self.c.fetchall()
        self.get_all_dates()
        return self.all

    def close_database_connection(self):
        self.conn.close()
        return True


class Notificationsender:
    def __init__(self):
        pass

    def send_notifications(self, daatta):
        pass


class Datamanipulator:
    def __init__(self, kok):
        pass

    def do_calculations(self, input_data):
        print(input_data)
        input_data+1
        return input_data


# -------------

is_notificable = False
data_address = "https://www.nordea.fi/wemapp/api/fi/lists/currency/electronicExchangeFI.dat"

cdata = CurrencyData(data_address)
rows_from_web = cdata.get_currency_pair_data("NOKEUR")
print(rows_from_web)
dbh = Databasehandler("/home/juhana/skriptit/currency_notif/data.db")
rows_from_database = dbh.get_all_from_database()
dates_from_database = dbh.all_dates

#    #2 7 8 9
lisatyt = 0
for item_web in rows_from_web:
    if item_web[2] not in dates_from_database:
        print("Puuttuu, lisätään.")
        dbh.push_to_database(item_web[2], item_web[7], item_web[8], item_web[9])
        dates_from_database.append(item_web[2])
        lisatyt += 1
        is_notificable = True
    else:
        print("On jo, ei lisätä.")

print("Lisätty: " + str(lisatyt))
dbh.close_database_connection()

#########
# Alla oleva ei toimi, pitää ensin valita notifikationlähetyspalvelu
#########

# Jos on uusia, lähetetään notifikaatiot
if is_notificable:
    # rivit tietokannasta uudelleen
    rows_from_database = dbh.get_all_from_database()

    dm = Datamanipulator()
    lahtevat_tiedot = dm.do_calculations(rows_from_database)  # tekee valmiin stringin

    ns = Notificationsender()
    ns.send_notifications(lahtevat_tiedot)

    dbh.close_database_connection()



print("done")

print("\n")
print("Newest rates:")
print("timecode\t\t\t", "\t", "cur", "-", "cur", "\t", "middle", "\t", "buying", "\t", "selling")
for rivi in rows_from_web:
    print(rivi[2], "\t", rivi[5], "-", rivi[6], "\t", rivi[7], "\t", rivi[8], "\t", rivi[9])
