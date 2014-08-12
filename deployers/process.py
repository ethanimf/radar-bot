import Image

# Frames come in 2 sizes
ROI_RECTS = {
    (640, 480): (1, 1, 478, 478),
    (672, 512): (1, 1, 510, 510)
}


def crop(image):
    return image.crop(ROI_RECTS[image.size])

# ## Extract Border
#
# The next step is to extract borders.
#
# Borders have 2 colors
BORDER_COLORS = [241, 243]
TRANSPARENT_COLOR = [7, 122, 205]

def make_radar_palette():
    p = []
    for i in range(255):
        p.append(i)
        p.append(i)
        p.append(i)
    # Transparent color
    p[0] = TRANSPARENT_COLOR[0]
    p[1] = TRANSPARENT_COLOR[1]
    p[2] = TRANSPARENT_COLOR[2]
    return p

BINARY_PALETTE = TRANSPARENT_COLOR + [0, 0, 0]
RADAR_PALETTE = make_radar_palette()

def extract_borders(image, border_colors = BORDER_COLORS):
    border_img = Image.new('P', image.size)
    border_img.putpalette(BINARY_PALETTE)
    radar_img = Image.new('P', image.size)
    radar_img.putpalette(RADAR_PALETTE)

    p = image.load()
    border_data = []
    radar_data = []
    for y in range(image.size[1]):
        for x in range(image.size[0]):
            c = p[x, y]
            if c in border_colors:
                border_data.append(1)
                radar_data.append(0)
            else:
                # 1 is 5dBz, 15 is 70+dBz
                border_data.append(0)
                radar_data.append(int(c * 255 / 15))
    border_img.putdata(border_data)
    radar_img.putdata(radar_data)
    return border_img, radar_img

# ## Morphology Operations
#
# In this section, some morphology operations are implemented.
# They are specialized for the purpose of this algorithm.
# Any color other than transparent (a.k.a `0`) will be consider as a valid pixel.
# Output images are binary.

# Convert any image to a binary one
def binarize(image):
    p = image.load()
    dst = Image.new('P', image.size)
    dst.putpalette(BINARY_PALETTE)
    data = []
    for y in range(image.size[1]):
        for x in range(image.size[0]):
            if p[x, y] == 0:
                data.append(0)
            else:
                data.append(1)
    dst.putdata(data)
    return dst


BOX_3 = [
    (-1, -1),
    (-1, 0),
    (-1, 1),
    (0, -1),
    (0, 0),
    (0, 1),
    (1, -1),
    (1, 0),
    (1, 1)
]
# Erosion
def erode(image):
    p = image.load()
    dst = Image.new('P', image.size)
    dst.putpalette(BINARY_PALETTE)
    data = []
    for y in range(image.size[1]):
        for x in range(image.size[0]):
            keep = True
            for d in BOX_3:
                bx = x + d[0]
                by = y + d[1]
                if bx < 0 or by < 0 or bx >= image.size[0] or by >= image.size[0]:
                    continue
                if p[bx, by] == 0:
                    keep = False
                    break
            if keep:
                data.append(1)
            else:
                data.append(0)
    dst.putdata(data)
    return dst

# Dilation
def dilate(image):
    p = image.load()
    dst = Image.new('P', image.size)
    dst.putpalette(BINARY_PALETTE)
    data = []
    for y in range(image.size[1]):
        for x in range(image.size[0]):
            keep = False
            for d in BOX_3:
                bx = x + d[0]
                by = y + d[1]
                if bx < 0 or by < 0 or bx >= image.size[0] or by >= image.size[0]:
                    continue
                if p[bx, by] != 0:
                    keep = True
                    break
            if keep:
                data.append(1)
            else:
                data.append(0)
    dst.putdata(data)
    return dst


# Open, avoid confusion with open file
def open_op(image):
    return dilate(erode(image))

# Close
def close_op(image):
    return erode(dilate(image))


# ## Mask for Inpaint
#
# Inpaint algorithm will be used to fill the gaps in a radar image caused by the removal of borders. Before that, one will need to provide inpaint algorithm with the inpaint mask, which specifies where the gaps are. In this case, the borders that passed through radar data region.
#
# It is quite straight forward to get the mask.
# First morphology operations are used to _expand_ the binary radar image. This step will fill the gaps.
# Then it is used as mask to substract borders that are not passing through data region.
def mask(image, mask):
    # image and mask should be the same size, too lazy to check
    dst = Image.new('P', image.size)
    dst.putpalette(BINARY_PALETTE)
    p = image.load()
    m = mask.load()
    data = []
    for y in range(image.size[1]):
        for x in range(image.size[0]):
            if m[x, y] == 0:
                data.append(0)
            else:
                data.append(p[x, y])
    dst.putdata(data)
    return dst

def get_inpaint_mask(radar_image, border_image):
    binary = binarize(radar_image)
    return mask(border_image, close_op(binary))


# ## Inpaint Algorithm
#
# In this section, a simplified version of OpenCV inpainting alorithm will be implemented. It is based on [An Image Inpainting Technique Based on the Fast Marching Method](http://iwi.eldoc.ub.rug.nl/FILES/root/2004/JGraphToolsTelea/2004JGraphToolsTelea.pdf).
#
# ### Priority Queues With Minimum Heap
#
# FMM inpainting uses a priority queue to manage a so-called "narrow-band" list.
#

import heapq
import itertools

class PriorityQueue:
    def __init__(self):
        self._q = []
        self._entry_map = {}
        self._counter = itertools.count()

    def push(self, priority, task):
        if task in self._entry_map:
            self.remove(task)
        count = next(self._counter)
        entry = [priority, count, task]
        self._entry_map[task] = entry
        heapq.heappush(self._q, entry)

    def remove(self, task):
        entry = self._entry_map.pop(task)
        entry[-1] = None

    def pop(self):
        while self._q:
            p, c, task = heapq.heappop(self._q)
            if task:
                del self._entry_map[task]
                return task
        return

    def empty(self):
        return len(self._q) == 0

import math
CROSS_3 = [
    (-1, 0),
    (1, 0),
    (0, 1),
    (0, -1)
]

def inpaint(image, mask):
    # Initialzie
    width = image.size[0]
    height = image.size[1]
    dst = Image.new('P', image.size)
    dst.putpalette(RADAR_PALETTE)
    data = [0] * width * height
    KNOWN = 0
    INSIDE = 255
    BAND = 1
    F = [0] * width * height
    D = [0] * width * height
    p = image.load()
    m = mask.load()
    narrow_band = PriorityQueue()
    # Helpers
    def index(x, y):
        return y * width + x
    def is_valid(bx, by):
        return bx >= 0 and by >= 0 and bx < width and by < height

    def _inpaint(x, y):
        b_e = []
        for d in BOX_3:
            if d[0] == 0 and d[1] == 0:
                continue
            nx = x + d[0]
            ny = y + d[1]
            if not is_valid(nx, ny):
                continue
            ni = index(nx, ny)
            # Spectial trick for this particular application
            # We know that KNOWN point cannot be 0
            if F[ni] == KNOWN and p[nx, ny] != 0:
                b_e.append(p[nx, ny])
        if len(b_e) != 0:
            # This is simplified comparing to original paper
            color = sum(b_e) / len(b_e)
            data[index(x, y)] = color

    def _solve(x1, y1, x2, y2):
        p1_valid = is_valid(x1, y1)
        p2_valid = is_valid(x2, y2)
        i1 = index(x1, y1)
        i2 = index(x2, y2)
        sol = float('inf')

        if p1_valid and F[i1] == KNOWN:
            t1 = D[i1]
            if p2_valid and F[i2] == KNOWN:
                t2 = D[i2]

                r = math.sqrt(2 * (t1 - t2) * (t1 - t2))
                s = (t1 + t2 * r) / 2
                if s >= t1 and s >= t2:
                    sol = s
                else:
                    s += r
                    if s >= t1 and s >= t2:
                        sol = s
            else:
                sol = 1 + t1
        elif p2_valid and F[i2] == KNOWN:
            t2 = D[i2]
            sol = 1 + t2
        return sol

    # Init mask
    for y in range(height):
        for x in range(width):
            i = index(x, y)
            flag = KNOWN
            if m[x, y] == 1:
                flag = INSIDE
            dist = 0.0
            n_total = 0
            n_unknown = 0
            if flag != KNOWN:
                for d in CROSS_3:
                    nx = x + d[0]
                    ny = y + d[1]
                    if not is_valid(nx, ny):
                        continue
                    n_total += 1
                    # Not known
                    if m[nx, ny] != 0:
                        n_unknown += 1
                #print "Point (%d, %d) Total N: %d, unknown: %d" % (x, y, n_total, n_unknown)
                if n_total > 0 and n_total == n_unknown:
                    flag = INSIDE
                    dist = float('inf')
                else:
                    flag = BAND

                    _inpaint(x, y)

                    narrow_band.push(dist, (x, y, i))
            F[i] = flag
            D[i] = dist

    # Inpaint narrow band
    while not narrow_band.empty():
        c = narrow_band.pop()

        F[c[2]] = KNOWN

        for d in CROSS_3:
            nx = c[0] + d[0]
            ny = c[1] + d[1]
            if not is_valid(nx, ny):
                continue
            ni = index(nx, ny)
            D[ni] = min([
                _solve(nx - 1, ny, nx, ny - 1),
                _solve(nx + 1, ny, nx, ny - 1),
                _solve(nx - 1, ny, nx, ny + 1),
                _solve(nx + 1, ny, nx, ny + 1)
            ])
            if F[ni] == INSIDE:
                F[ni] = BAND
                _inpaint(nx, ny)
                narrow_band.push(D[ni], (nx, ny, ni))

    # Fill known pixels
    for y in range(height):
        for x in range(width):
            if m[x, y] != 1:
                data[index(x, y)] = p[x, y]

    dst.putdata(data)
    return dst

from cStringIO import StringIO
import base64
import logging

def run_crop_only(image):
  logging.info("Start cropping frame")
  final = crop(image)
  logging.info("Finish cropping frame")
  output = StringIO()
  final.save(output, format = 'PNG', transparency = 0)
  return base64.encodestring(output.getvalue())

def run(image):
  logging.info("Start processing frame")
  border, radar = extract_borders(crop(image))
  mask = get_inpaint_mask(radar, border)
  final = inpaint(radar, mask)
  logging.info("Finish processing frame")
  output = StringIO()
  final.save(output, format = 'PNG', transparency = 0)
  return base64.encodestring(output.getvalue())

if __name__ == '__main__':
  import urllib2
  url = 'http://image.weather.gov.cn/product/2014/201408/20140811/RDCP/SEVP_AOC_RDCP_SLDAS_EBREF_AZ9230_L88_PI_20140811142500000.GIF?v=1407767698299'
  fd = urllib2.urlopen(url)
  image = Image.open(StringIO(fd.read()))
  print run(image)
