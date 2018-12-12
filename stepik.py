import itertools

G = {1:[6,7,8], 2: [5,7,8], 3: [5,6,8], 4:[5,6,7],
     5:[2,3,4], 6:[1,3,4], 7:[1,2,4], 8:[1,2,3]}

COLORS = set(G.keys()) 
def color_it(order):
    def get_neighb_colors(v, dict_colors):
        return {dict_colors[n] for n in G[v] if dict_colors[n]!=None}
    cur_colors = dict.fromkeys(G.keys(), None)
    for v in order:
        cur_colors[v] = min(COLORS - get_neighb_colors(v, cur_colors))
    return set(cur_colors.values())

for order in itertools.permutations(G.keys()):
    if  len(color_it(order))==4:
        print(order)
        break
