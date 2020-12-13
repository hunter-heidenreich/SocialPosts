def display_num(num):
    if num > 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.1f} T"
    elif num > 1_000_000_000:
        return f"{num / 1_000_000_000:.1f} B"
    elif num > 1_000_000:
        return f"{num / 1_000_000:.1f} M"
    elif num > 1_000:
        return f"{num / 1_000:.1f} K"
    else:
        return f"{num:.1f}"
