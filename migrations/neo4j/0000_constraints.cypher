// COMPANY
CREATE CONSTRAINT company_name_unique IF NOT EXISTS
FOR (c:Company) REQUIRE c.name IS UNIQUE;

CREATE CONSTRAINT company_ticker_unique IF NOT EXISTS
FOR (c:Company) REQUIRE c.ticker IS UNIQUE;

// COUNTRY
CREATE CONSTRAINT country_iso_num_unique IF NOT EXISTS
FOR (c:Country) REQUIRE c.iso_num IS UNIQUE;

// THEME
CREATE CONSTRAINT theme_name_unique IF NOT EXISTS
FOR (t:Theme) REQUIRE t.name IS UNIQUE;