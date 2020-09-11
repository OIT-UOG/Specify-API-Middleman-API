import unittest

def sort_place(a):
    na = []
    aa = []
    for c, i in enumerate(a):
        for k, v in i.items():
            if v is None:
                na.append({k: c})
            else:
                aa.append(i)
    sa = sorted(aa, key=lambda i: i[list(i)[0]])
    for n in na:
        sa.insert(n[list(n)[0]], {list(n)[0]: None})
    return sa


def merge(a, b):
    """given lists of single item dicts where values are None or integers
    sorts the lists asc without moving Nones
    returns merged list bumping numbers as necessary
    on conflicts where additional info is needed, alphabetical key sorting is used
    """
    a = sort_place(a)
    b = sort_place(b)
    ai = 0
    bi = 0
    d = None
    wina = []
    winb = []
    bk = {}
    conflict = False
    ret = []
    while True:
        try:
            x = a[ai]
        except:
            x = {None: None}
        try:
            y = b[bi]
        except:
            y = {None: None}

        nai = 0
        bai = 0
        xk = list(x)[0]
        yk = list(y)[0]
        xv = x[xk]
        yv = y[yk]

        # end
        if xk is None and yk is None and not conflict:
            break

        # no conflict
        if not conflict:
            if xk == yk:
                # both keys and values are same (match)
                if xv == yv:
                    # current val counter = value or ++
                    if None in [xv, d] or d < xv:
                        d = xv
                    ret.append({xk: d})
                    if d is not None:
                        d += 1
                # keys are same but values differ
                else:
                    # val counter = max value
                    d = max(filter(None, [xv,yv,d]))
                    ret.append({xk: d})
                    d += 1
                ai += 1
                bi += 1
            # differing keys. conflict! or one of the lists ran out
            else:
                conflict = True
                # set up windows
                wina = [xk]
                winb = [yk]
                # and store key's values
                bk = {xk: xv, yk: yv}
                nai = ai
                bai = bi
                ai += 1
                bi += 1
        # there was a conflict in the past
        else:
            # if there is an item, put it in the window
            if xk is not None:
                wina.append(xk)
                ai += 1
            if yk is not None:
                winb.append(yk)
                bi += 1

            # avoiding None since xk/yk can be None, 
            # but really they shouldn't be here. check later
            found = False

            foundv = None

            # see if key in opposite window
            if xk in winb:
                found = xk
                foundv = xv
            if yk in wina:
                found = yk
                foundv = yv
            # does this overwrite anything?
            if not found:
                bk[xk] = xv
                bk[yk] = yv
            
            out = xk is None and yk is None and not found

            if found is not False or out:
                # crop windows up until found value
                posa = []
                posb = []
                for p, w in [(posa, wina), (posb, winb)]:
                    for k in w:
                        if k == found:
                            break
                        p.append(k)    
                wina = wina[len(posa)+1:]
                winb = winb[len(posb)+1:]
                # sort cropped windows based on 1st value
                posa, posb = sorted([posa,posb], key=lambda i: '' if len(i)==0 else i[0])
                # add them to the return
                FOUND_ONE = {found: foundv}
                for p in [*posa, *posb, FOUND_ONE]:
                    if p is FOUND_ONE:
                        p = found
                        pv = foundv
                        if out:
                            break
                    else:
                        pv = bk[p]

                    if None in [pv, d] or d < pv:
                        d = pv
                    ret.append({p: d})
                    if d is not None:
                        d += 1
                # backtrack to resume point
                ai -= len(wina)
                bi -= len(winb)
                wina = []
                winb = []
                conflict = False
                    
                    
            else:
                pass
                

        # # same
        # if list(x) == list(y):
        #     if xv is None and yv is None:
        #         ret.append({xk: d})
        #         if d is not None:
        #             d+=1
        #     else:
        #         if d is not None:
        #             ret.append({xk: d})
        #             d+=1
        #             continue
        #         not_nones = [i for i in [xv,yv] if i is not None]
        #         if len(not_nones) == 1:
    return ret



class TestMerge(unittest.TestCase):
    def test_sort(self):
        a = [
            {'bob': None},
            {'aob': None},
            {'john': 0},
            {'jen': 2},
            {'asd': None},
            {'andy': 1},
            {'tum': 10},
            {'tim': 3},
            {'work': 6},
            {'no': 4},
            {'gum': 8},
            {'go': 5},
            {'pen': 7},
            {'mug': 9},
            {'hit': None},
            {'aja': 11},
            {'mm': None},
            {'nn': None}
        ]
        self.assertEqual(sort_place(a), [
            {'bob': None},
            {'aob': None},
            {'john': 0},
            {'andy': 1},
            {'asd': None},
            {'jen': 2},
            {'tim': 3},
            {'no': 4},
            {'go': 5},
            {'work': 6},
            {'pen': 7},
            {'gum': 8},
            {'mug': 9},
            {'tum': 10},
            {'hit': None},
            {'aja': 11},
            {'mm': None},
            {'nn': None}
        ])
    def test_merge(self):
        a = [
            {'bob': None},
            {'john': 0},
            {'andy': 1},
            {'dome': None},
            {'jen': 2},
            {'tim': 3},
            {'no': 4},
            {'go': 5},
            {'work': 6},
            {'pen': 7},
            {'gum': 8},
            {'mug': 9},
            {'tum': 10},
            {'hit': None},
            {'mm': None},
            {'nn': None}
        ]
        b = [
            {'bob': None},
            {'john': 0},
            {'andy': 2},
            {'k': 1},
            {'dome': None},
            {'foam': None},
            {'dog': 3},
            {'tim': 4},
            {'work': 5},
            {'mun': 6},
            {'hit': None},
            {'bit': None}
        ]
        self.assertEqual(merge(a, b), [
            {'bob': None},
            {'john': 0},
            {'k': 1},
            {'andy': 2},
            {'dome': None},
            {'foam': None},
            {'dog': 3},
            {'jen': 4},
            {'tim': 5},
            {'no': 6},
            {'go': 7},
            {'work': 8},
            {'mun': 9},
            {'pen': 10},
            {'gum': 11},
            {'mug': 12},
            {'tum': 13},
            {'hit': None},
            {'bit': None},
            {'mm': None},
            {'nn': None}
        ])

if __name__ == "__main__":
    unittest.main()