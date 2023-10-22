import functools
import itertools
import logging
import os
import shutil
import subprocess
import time
import uuid
import requests
import random
import urllib
from func_timeout import func_timeout, FunctionTimedOut

import gmpy2


def factor(composites, threads=1, work=None, pretest=None, one=None, timeout=None):
    if not timeout:
        return factor_implementation(composites, threads=threads, work=work, pretest=pretest, one=one)
    return func_timeout(timeout, factor_implementation, [composites],
                        kwargs={"threads": threads, "work": work, "one": one, "pretest": pretest})


def factor_implementation(composites, threads=1, work=None, pretest=None, one=None):
    expr = "".join([f"factor({composite})\n" for composite in composites])
    logging.info(f"Running yafu for {len(composites)} composites")
    # [logging.info(f"{len(composite) if type(composite) == str else gmpy2.num_digits(composite)}-digit composite: {composite}") for composite in composites]
    start_time = time.time()
    this_uuid = uuid.uuid4()
    yafu_dir = "C:/Software/yafu-master/"
    dirpath = os.path.join("temp", str(this_uuid))
    filename = f"temp-{this_uuid}.dat"
    temp_filepath = os.path.join(dirpath, filename)
    proc = None
    try:
        os.makedirs(dirpath, exist_ok=True)
        # copy the yafu.ini so we can use it from the throwaway temp dir without polluting the yafu directory with
        # logs and restart points
        shutil.copy(os.path.join(yafu_dir, "yafu.ini"), os.path.join(dirpath, "yafu.ini"))
        with open(temp_filepath, "w") as temp:
            temp.write(f"{expr}\n")
        factors_filename = "factors.out"
        popen_arglist = [
            os.path.join(yafu_dir, "yafu-x64-gc.exe"), "-of", factors_filename, "-no_expr",
        ]
        if threads != 1:
            popen_arglist.append("-threads")
            popen_arglist.append(str(threads))
        if work:
            popen_arglist.append("-work")
            popen_arglist.append(str(work))
        if pretest:
            popen_arglist.append("-pretest")
            popen_arglist.append(str(pretest))
        if one:
            popen_arglist.append("-one")

        proc = subprocess.Popen(popen_arglist,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE,
                                universal_newlines=True, cwd=dirpath, bufsize=1)
        print(expr, file=proc.stdin, flush=True)
        proc.stdin.close()
        for i, line in enumerate(proc.stdout):
            logging.debug(line[:-1])
        proc.wait()
    finally:
        if proc:
            proc.kill()
        # send factors even if killed early
        to_report = []
        factors = []
        with open(os.path.join(dirpath, factors_filename), "r") as factors_file:
            for line in factors_file.readlines():
                factors = line.split("/")  # [2, 3^5, ...]
                factors = [term.split("^") for term in factors]  # [[2], [3, 5], ...]
                factors = [[gmpy2.mpz(term[0])] if len(term) == 1 else [gmpy2.mpz(term[0])] * int(term[1]) for term in factors]  # [[2], [3, 3, 3, 3, 3], ...]
                factors = list(itertools.chain.from_iterable(factors))
                to_report.append((factors[0], factors[1:]))
            elapsed = time.time() - start_time
            report(to_report)
            logging.debug(f"yafu ran {expr} in {elapsed:.02f} seconds")
        # cleanup temp dir
        shutil.rmtree(dirpath, ignore_errors=True)


def report(composite_factors_tuples):
    payload = "\n".join(["{}={}".format(composite, str([int(factor) for factor in factors])) for composite, factors in composite_factors_tuples])
    logging.info(f"submitting factors:\n{payload}")
    payload = 'report=' + urllib.parse.quote(payload, safe='') + '&format=0'
    payload = payload.encode('utf-8')
    temp2 = urllib.request.urlopen('http://factordb.com/report.php', payload)
    if temp2.status != 200:
        raise Exception(temp2)


def get_composites(minimum_digits=1, num_composites=100, start_number=0):
    request = requests.get(f"http://factordb.com/listtype.php?t=3&mindig={minimum_digits}&perpage={num_composites}&start={start_number}&download=1")
    if not request.ok:
        raise ConnectionError(f"Problem with request:\n{request.text}")
    return request.text.strip().split('\n')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # clear temp files when starting
    shutil.rmtree(os.path.join("temp"), ignore_errors=True)

    trivial_size = 70
    threads = 1
    num_composites = 20
    num_digits = 0

    shaver = False

    shave_composites = 50
    shaver_threads = 16
    shaver_digits = 78
    shaver_work = None
    shave_depth = 20

    if shaver:  # "shaver" runs a bit of ecm on lots of composites to smooth out the graph
        seen_composites = set()
        while True:
            try:
                # 50000 is the highest start_number allowed
                string_composites = get_composites(minimum_digits=shaver_digits, num_composites=shave_composites, start_number=random.randrange(50000))
                composites = list(filter(lambda composite: composite not in seen_composites, map(gmpy2.mpz, string_composites)))
                # hash the composites we've seen, so we don't waste effort on them again
                seen_composites.update(composites)
                if len(composites) == 0:
                    continue
                factor(composites, threads=shaver_threads, work=shaver_work, pretest=shave_depth, one=True)
            except (FileNotFoundError, UnicodeDecodeError) as e:
                logging.error(e)
    else:
        while True:
            try:
                string_composites = get_composites(minimum_digits=num_digits, num_composites=num_composites, start_number=random.randrange(1000))
                start = time.time()
                trivial_composites = list(filter(lambda x: len(x) < trivial_size, string_composites))
                if trivial_composites:
                    factor(trivial_composites, threads=threads)
                else:
                    # choose random composite of all composites with smallest amount of digits
                    digit_size_dict = {}
                    for string_composite in string_composites:
                        composite_list = digit_size_dict.get(len(string_composite), [])
                        composite_list.append(string_composite)
                        digit_size_dict[len(string_composite)] = composite_list
                    composite_list = digit_size_dict[sorted(digit_size_dict.keys())[0]]
                    composite = [random.choice(composite_list)]
                    factor(composite, threads=threads, timeout=30)
            except (FileNotFoundError, UnicodeDecodeError, FunctionTimedOut) as e:
                logging.error(e)
