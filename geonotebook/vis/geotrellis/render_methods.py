from PIL import Image

def hex_to_rgb(value):
    """Return (red, green, blue) for the color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

nlcd_color_map =  { 11 : "#526095",     # Open Water
                    12 : "#FFFFFF",     # Perennial Ice/Snow
                    21 : "#D28170",     # Low Intensity Residential
                    22 : "#EE0006",     # High Intensity Residential
                    23 : "#990009",     # Commercial/Industrial/Transportation
                    31 : "#BFB8B1",     # Bare Rock/Sand/Clay
                    32 : "#969798",     # Quarries/Strip Mines/Gravel Pits
                    33 : "#382959",     # Transitional
                    41 : "#579D57",     # Deciduous Forest
                    42 : "#2A6B3D",     # Evergreen Forest
                    43 : "#A6BF7B",     # Mixed Forest
                    51 : "#BAA65C",     # Shrubland
                    61 : "#45511F",     # Orchards/Vineyards/Other
                    71 : "#D0CFAA",     # Grasslands/Herbaceous
                    81 : "#CCC82F",     # Pasture/Hay
                    82 : "#9D5D1D",     # Row Crops
                    83 : "#CD9747",     # Small Grains
                    84 : "#A7AB9F",     # Fallow
                    85 : "#E68A2A",     # Urban/Recreational Grasses
                    91 : "#B6D8F5",     # Woody Wetlands
                    92 : "#B6D8F5" }    # Emergent Herbaceous Wetlands

def nlcd_color_palette():
    palette = [0, 0, 0] * 100
    for key in nlcd_color_map:
        i = key * 3
        (r, g, b) = hex_to_rgb(nlcd_color_map[key])
        palette[i] = r
        palette[i + 1] = g
        palette[i + 2] = b
    return palette

def render_nlcd(tile):
    palette = nlcd_color_palette()

    img = Image.fromarray(tile, mode='P')
    img.putpalette(palette)

    return img
