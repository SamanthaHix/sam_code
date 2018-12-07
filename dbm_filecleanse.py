import os
import csv
import sys

def csv_rec_iter(filenm):
    data = []
    print("NOTE: started reading {}".format(filenm))
    cnt = 0
    with open(filenm) as fp:
        rdr = csv.reader(fp, dialect="excel")
        hdr = trim_row(next(rdr))
        for row in rdr:
            cnt += 1
            yield dict(zip(hdr, trim_row(row)))
    print("NOTE: read {} records from {}".format(cnt, filenm))

def fix_dbm_dt(s):
    y,m,d = s.split("/")
    return "/".join([m,d,y])

def dbm_iter(filenm):
    for rec in csv_rec_iter(filenm):
        out_rec = {}
        if rec["Date"] == "":
            return
        rec["Date"] = fix_dbm_dt(rec["Date"])
        
        yield out_rec

def mk_output(iters, out_filenm):
    print("NOTE: started writing {}".format(out_filenm))
    hdr = [
        "Advertiser Currency",
        "Creative",
        "Date",
        "Advertiser",
        "Campaign",
        "Line Item",
        "Revenue (Adv Currency) USD"
    ]
    cnt = 0
    with open(out_filenm, "w") as fp_out:
        wtr = csv.writer(fp_out, dialect="excel")
        wtr.writerow(hdr)
        for xiter in iters:
            for rec in xiter:
                row = []
                for fld in hdr:
                    row.append(rec.get(fld, "(missing)"))
                wtr.writerow(row)
                cnt += 1
    print("NOTE: wrote {} records to {}".format(cnt, out_filenm))
 

if __name__=="__main__":
    in_dir = "/home/samanthah/Nissan"
    in_dir = ""

    dbm_filenm = os.path.join(in_dir, "dbm_performance.csv")

    out_filenm = "dbm_output.csv"

    iters = [

        dbm_iter(dbm_filenm)
    ]

    mk_output(iters, out_filenm)


