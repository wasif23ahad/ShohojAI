-- Seed sample Bangladeshi e-commerce catalog products
INSERT INTO products (sku, name_en, name_bn, description, price_bdt, stock_qty, variants)
VALUES
    (
        'HD-BLK-001',
        'Premium Black Fleece Hoodie',
        'প্রিমিয়াম ব্ল্যাক ফ্লিস হুডি',
        'Heavyweight 320 GSM winter cotton fleece hoodie in jet black. Soft brushed inside, kangaroo pocket, durable ribbed cuffs.',
        1250.00,
        25,
        '{"sizes":{"M":5,"L":8,"XL":6,"XXL":6},"category":"hoodie","color":"black"}'::jsonb
    ),
    (
        'PNJ-WHT-002',
        'Embroidered Cotton Panjabi',
        'এমব্রয়ডারি সুতি পাঞ্জাবি',
        '100% breathable combed cotton white panjabi with subtle collar and placket embroidery. Ideal for Friday prayers and festive occasions.',
        1850.00,
        20,
        '{"sizes":{"40":5,"42":8,"44":7},"category":"panjabi","color":"white"}'::jsonb
    ),
    (
        'SAR-JMD-003',
        'Dhakai Jamdani Saree',
        'ঢাকাই জামদানি শাড়ি',
        'Authentic handloom cotton-silk Dhakai Jamdani saree in crimson red and golden zari motif.',
        4500.00,
        10,
        '{"category":"saree","color":"red-gold"}'::jsonb
    ),
    (
        'TSH-NVY-004',
        'Heavyweight Drop-Shoulder T-Shirt',
        'ড্রপ-শোল্ডার কটন টি-শার্ট',
        '220 GSM organic ring-spun cotton oversized drop-shoulder t-shirt in navy blue.',
        650.00,
        40,
        '{"sizes":{"M":10,"L":15,"XL":10,"XXL":5},"category":"t-shirt","color":"navy"}'::jsonb
    ),
    (
        'CHN-KHK-005',
        'Stretchable Gabardine Chinos',
        'স্ট্রেচেবল গ্যাবার্ডিন চিনোস প্যান্ট',
        'Slim-fit cotton spandex stretchable gabardine trouser in classic khaki.',
        1150.00,
        30,
        '{"sizes":{"30":6,"32":10,"34":8,"36":6},"category":"pant","color":"khaki"}'::jsonb
    )
ON CONFLICT (sku) DO NOTHING;
