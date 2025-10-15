-- ============================================================================
-- ENG-1973 Part 4: Create Email Domains Reference Table
-- Purpose: Create and populate domain classification table for email type detection
-- ============================================================================

-- Drop table if exists (for idempotency)
DROP TABLE IF EXISTS derived.vector_email_domains CASCADE;

-- Create domains reference table
CREATE TABLE derived.vector_email_domains (
    domain VARCHAR(255) PRIMARY KEY,         -- Email domain (lowercase)
    type VARCHAR(20) NOT NULL,               -- 'company', 'personal', or 'spam'
    vuc_id VARCHAR(32),                      -- Reference to vector_universal_company (for company domains)
    notes VARCHAR(500),                      -- Additional context
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL;                               -- Small reference table, replicate to all nodes

-- Populate company domains from vector_universal_company
INSERT INTO derived.vector_email_domains (domain, type, vuc_id, notes)
SELECT DISTINCT
    LOWER(TRIM(domain)) as domain,
    'company' as type,
    vuc_id,
    'Domain from vector_universal_company' as notes
FROM derived.vector_universal_company
WHERE domain IS NOT NULL
  AND domain != ''
  AND domain NOT LIKE '%@%';                 -- Exclude malformed entries

-- Populate common personal email domains
INSERT INTO derived.vector_email_domains (domain, type, notes)
VALUES
    ('gmail.com', 'personal', 'Google personal email'),
    ('yahoo.com', 'personal', 'Yahoo personal email'),
    ('hotmail.com', 'personal', 'Microsoft Hotmail'),
    ('outlook.com', 'personal', 'Microsoft Outlook'),
    ('aol.com', 'personal', 'AOL personal email'),
    ('icloud.com', 'personal', 'Apple iCloud'),
    ('me.com', 'personal', 'Apple Me email'),
    ('mac.com', 'personal', 'Apple Mac email'),
    ('live.com', 'personal', 'Microsoft Live'),
    ('msn.com', 'personal', 'Microsoft MSN'),
    ('protonmail.com', 'personal', 'ProtonMail secure email'),
    ('proton.me', 'personal', 'ProtonMail rebranded'),
    ('mail.com', 'personal', 'Mail.com generic email'),
    ('zoho.com', 'personal', 'Zoho personal email'),
    ('yandex.com', 'personal', 'Yandex email'),
    ('gmx.com', 'personal', 'GMX email'),
    ('gmx.net', 'personal', 'GMX email'),
    ('fastmail.com', 'personal', 'FastMail'),
    ('qq.com', 'personal', 'Tencent QQ (China)'),
    ('163.com', 'personal', 'NetEase (China)'),
    ('126.com', 'personal', 'NetEase (China)'),
    ('sina.com', 'personal', 'Sina (China)'),
    ('sohu.com', 'personal', 'Sohu (China)'),
    ('yeah.net', 'personal', 'NetEase (China)'),
    ('mail.ru', 'personal', 'Mail.ru (Russia)'),
    ('inbox.ru', 'personal', 'Mail.ru (Russia)'),
    ('list.ru', 'personal', 'Mail.ru (Russia)'),
    ('bk.ru', 'personal', 'Mail.ru (Russia)'),
    ('web.de', 'personal', 'Web.de (Germany)'),
    ('orange.fr', 'personal', 'Orange (France)'),
    ('free.fr', 'personal', 'Free (France)'),
    ('laposte.net', 'personal', 'La Poste (France)'),
    ('wanadoo.fr', 'personal', 'Wanadoo (France)'),
    ('libero.it', 'personal', 'Libero (Italy)'),
    ('virgilio.it', 'personal', 'Virgilio (Italy)'),
    ('tin.it', 'personal', 'TIN (Italy)'),
    ('tiscali.it', 'personal', 'Tiscali (Italy)'),
    ('alice.it', 'personal', 'Alice (Italy)'),
    ('t-online.de', 'personal', 'T-Online (Germany)'),
    ('bluewin.ch', 'personal', 'Swisscom (Switzerland)'),
    ('telenet.be', 'personal', 'Telenet (Belgium)'),
    ('skynet.be', 'personal', 'Proximus (Belgium)'),
    ('bt.com', 'personal', 'BT (UK)'),
    ('btinternet.com', 'personal', 'BT (UK)'),
    ('sky.com', 'personal', 'Sky (UK)'),
    ('ntlworld.com', 'personal', 'Virgin Media (UK)'),
    ('blueyonder.co.uk', 'personal', 'Virgin Media (UK)'),
    ('talktalk.net', 'personal', 'TalkTalk (UK)'),
    ('comcast.net', 'personal', 'Comcast (USA)'),
    ('verizon.net', 'personal', 'Verizon (USA)'),
    ('att.net', 'personal', 'AT&T (USA)'),
    ('sbcglobal.net', 'personal', 'AT&T SBC (USA)'),
    ('bellsouth.net', 'personal', 'AT&T BellSouth (USA)'),
    ('cox.net', 'personal', 'Cox (USA)'),
    ('earthlink.net', 'personal', 'EarthLink (USA)'),
    ('charter.net', 'personal', 'Charter (USA)'),
    ('optonline.net', 'personal', 'Optimum (USA)'),
    ('rr.com', 'personal', 'Spectrum (USA)'),
    ('roadrunner.com', 'personal', 'Spectrum (USA)'),
    ('windstream.net', 'personal', 'Windstream (USA)'),
    ('frontier.com', 'personal', 'Frontier (USA)'),
    ('centurylink.net', 'personal', 'CenturyLink (USA)'),
    ('juno.com', 'personal', 'Juno (USA)'),
    ('netzero.net', 'personal', 'NetZero (USA)'),
    ('aim.com', 'personal', 'AIM (USA)'),
    ('rogers.com', 'personal', 'Rogers (Canada)'),
    ('bell.net', 'personal', 'Bell (Canada)'),
    ('shaw.ca', 'personal', 'Shaw (Canada)'),
    ('telus.net', 'personal', 'Telus (Canada)'),
    ('sympatico.ca', 'personal', 'Bell Sympatico (Canada)');

-- Populate common spam/disposable email domains
INSERT INTO derived.vector_email_domains (domain, type, notes)
VALUES
    ('mailinator.com', 'spam', 'Disposable email service'),
    ('guerrillamail.com', 'spam', 'Disposable email service'),
    ('10minutemail.com', 'spam', 'Disposable email service'),
    ('temp-mail.org', 'spam', 'Disposable email service'),
    ('throwaway.email', 'spam', 'Disposable email service'),
    ('trashmail.com', 'spam', 'Disposable email service'),
    ('dispostable.com', 'spam', 'Disposable email service'),
    ('tempmail.com', 'spam', 'Disposable email service'),
    ('yopmail.com', 'spam', 'Disposable email service'),
    ('fakeinbox.com', 'spam', 'Disposable email service'),
    ('maildrop.cc', 'spam', 'Disposable email service'),
    ('getnada.com', 'spam', 'Disposable email service'),
    ('mohmal.com', 'spam', 'Disposable email service'),
    ('sharklasers.com', 'spam', 'Disposable email service'),
    ('spamgourmet.com', 'spam', 'Disposable email service'),
    ('mailcatch.com', 'spam', 'Disposable email service'),
    ('emailondeck.com', 'spam', 'Disposable email service'),
    ('mintemail.com', 'spam', 'Disposable email service'),
    ('mytemp.email', 'spam', 'Disposable email service'),
    ('temp-mail.io', 'spam', 'Disposable email service');

-- Record domains table creation in metadata
INSERT INTO derived.vector_email_migration_metadata_eng1973 
    (migration_step, records_affected, notes)
VALUES 
    ('domains_table_created', 
     (SELECT COUNT(*) FROM derived.vector_email_domains),
     'Created and populated vector_email_domains reference table with company, personal, and spam domains');

-- Verify domains table
SELECT 
    type,
    COUNT(*) as domain_count,
    COUNT(vuc_id) as with_company_reference
FROM derived.vector_email_domains
GROUP BY type
ORDER BY type;

-- Sample domains by type
SELECT 
    type,
    LISTAGG(domain, ', ') WITHIN GROUP (ORDER BY domain) as sample_domains
FROM (
    SELECT 
        type,
        domain,
        ROW_NUMBER() OVER (PARTITION BY type ORDER BY domain) as rn
    FROM derived.vector_email_domains
)
WHERE rn <= 10
GROUP BY type
ORDER BY type;

