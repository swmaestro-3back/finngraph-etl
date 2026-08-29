import html
import re
from typing import Any

ARTICLE_BODY_HTML_BREAK_PATTERN = re.compile(
    r"<\s*/?\s*(?:br|p|div|section|article|li|ul|ol|h[1-6]|blockquote)\b[^>]*>",
    re.IGNORECASE,
)
ARTICLE_BODY_TRANSLATION_TABLE = str.maketrans(
    {
        "\u00a0": " ",
        "\u200b": "",
        "\u200c": "",
        "\u200d": "",
        "\ufeff": "",
        "｜": "|",
        "│": "|",
        "┃": "|",
        "［": "[",
        "］": "]",
        "【": "[",
        "】": "]",
        "〈": "[",
        "〉": "]",
        "《": "[",
        "》": "]",
        "＜": "[",
        "＞": "]",
        "（": "(",
        "）": ")",
        "：": ":",
    }
)
ARTICLE_BODY_PERSON_PATTERN_TEXT = r"(?:[가-힣]{2,12}|[A-Z][A-Za-z.' -]{1,50})"
ARTICLE_BODY_ROLE_PATTERN_TEXT = (
    r"(?:선임기자|전문기자|수습기자|객원기자|인턴기자|촬영기자|사진기자|"
    r"기자|특파원|편집자|앵커|논설위원|에디터|PD|피디|리포터|통신원)"
)
ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT = (
    rf"{ARTICLE_BODY_PERSON_PATTERN_TEXT}"
    rf"(?:\s*[·,&/]\s*{ARTICLE_BODY_PERSON_PATTERN_TEXT})*"
)
ARTICLE_BODY_AD_LABEL_PATTERN_TEXT = (
    r"(?:광고|유료\s*광고|협찬|PR|PPL|ad|advertisement|sponsored|"
    r"스폰서드|프로모션|브랜드\s*콘텐츠)"
)
ARTICLE_BODY_AD_LINE_PATTERN = re.compile(
    rf"^\s*(?:[\[(]\s*{ARTICLE_BODY_AD_LABEL_PATTERN_TEXT}\s*[\])]"
    rf"(?:\s+.*)?|{ARTICLE_BODY_AD_LABEL_PATTERN_TEXT}"
    r"(?:\s*[:=\-]\s*.*)?)\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_MEDIA_LABEL_LINE_PATTERN = re.compile(
    r"^\s*['\"“”‘’]?\s*"
    r"(?:(?:[^.!?。\n]{1,100}\s+)?"
    r"(?:사진|포토|이미지|영상|그래픽|자료\s*사진|참고\s*사진|"
    r"기사\s*사진|대표\s*이미지|사진\s*출처|이미지\s*출처|"
    r"사진\s*제공|이미지\s*제공)|"
    r"(?:사진|이미지)\s*(?:출처|제공))"
    r"\s*['\"“”‘’]?\s*(?:[:=]\s*[^\n]{0,160})?[.!]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_COMPACT_MEDIA_CREDIT_LINE_PATTERN = re.compile(
    r"^\s*[^\n]{0,140}?\s*(?:[/|·▶▷-]\s*)?"
    r"['\"“”‘’]?\s*(?:사진|포토|이미지|영상|그래픽)\s*"
    r"(?:출처|제공)?\s*['\"“”‘’]?\s*[:=]\s*[^.!?。\n]{0,100}\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_MEDIA_CREDIT_PATTERN = re.compile(
    r"^\s*(?:['\"“”‘’]?\s*[^|.!?。\n]{0,100}?\s*"
    r"(?:사진|포토|이미지|자료\s*사진|사진\s*출처|이미지\s*출처|"
    r"사진\s*제공|이미지\s*제공)\s*['\"“”‘’]?"
    r"(?:[:=]\s*[^|.!?。\n]{0,100})?|"
    r"(?:출처|자료\s*제공|화면\s*출처|이미지\s*출처|사진\s*출처)"
    r"\s*[:=]\s*[^|.!?。\n]{1,120})"
    r"\s*[|·▶▷-]+\s*",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_IMAGE_CAPTION_PATTERN = re.compile(
    r"^\s*(?:(?:위|아래|해당|이|본|관련)\s*)?"
    r"(?:사진|이미지|자료\s*사진|화면|장면|사진\s*속|이미지\s*속)"
    r"(?:은|는|이|가|에는|에서|으로)?\s+[^\n]{1,240}$|"
    r"^\s*[^\n]{1,180}\s+"
    r"(?:제공|제공한|캡처|갈무리|촬영|출처|자료\s*사진)\s*[.!]?\s*$|"
    r"^\s*(?:사진|이미지)\s*(?:크게\s*보기|확대|원본\s*보기)\s*$|"
    r"^\s*(?:기사|본문)\s*내용과\s*(?:직접적인\s*)?관련\s*(?:없는|없음)"
    r"[^\n]{0,120}(?:사진|이미지)\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_LOCATION_SOURCE_LINE_PATTERN = re.compile(
    r"^\s*[([]?\s*(?:[가-힣]{2,12}|SEOUL|NEW\s+YORK|WASHINGTON|TOKYO|"
    r"BEIJING|LONDON)\s*[=/]\s*"
    r"[^\])\]\n]{0,60}(?:뉴스|신문|일보|통신|방송|TV|REUTERS|AP|AFP|"
    r"BLOOMBERG)[^\])\]\n]{0,30}\s*[)\]]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_LOCATION_SOURCE_PATTERN = re.compile(
    r"^\s*[([]?\s*(?:[가-힣]{2,12}|SEOUL|NEW\s+YORK|WASHINGTON|TOKYO|"
    r"BEIJING|LONDON)\s*[=/]\s*"
    r"[^\])\]\n]{0,60}(?:뉴스|신문|일보|통신|방송|TV|REUTERS|AP|AFP|"
    r"BLOOMBERG)[^\])\]\n]{0,30}\s*[)\]]?\s*"
    r"(?:[|:·▶▷-]+\s*|(?=[가-힣A-Za-z0-9]))",
    re.IGNORECASE,
)
ARTICLE_BODY_BARE_MEDIA_NAME_PATTERN = re.compile(
    r"^\s*(?:[가-힣A-Za-z0-9&·.\- ]{1,50}"
    r"(?:뉴스|신문|일보|통신|방송|TV|타임즈|투데이|저널|데일리)|"
    r"한국경제|매일경제|서울경제|헤럴드경제|아시아경제|이데일리|"
    r"머니투데이|뉴시스|뉴스1|연합뉴스|조선비즈|비즈워치|더팩트|"
    r"데일리안|REUTERS|BLOOMBERG|ASSOCIATED\s+PRESS|AP|AFP)\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_STANDALONE_SOURCE_CREDIT_PATTERN = re.compile(
    r"^\s*(?:사진|이미지|화면|자료)?\s*"
    r"(?:출처|제공|캡처|갈무리|촬영)\s*[:=]\s*[^\n]{1,180}$|"
    r"^\s*[^\n]{1,150}\s*(?:홈페이지|SNS|유튜브|인스타그램|페이스북|"
    r"보도자료)\s*(?:캡처|갈무리|제공)\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_READER_TOOL_PATTERN = re.compile(
    r"^\s*(?:기사\s*듣기|음성으로\s*듣기|본문\s*듣기|AI\s*(?:요약|에게\s*질문)|"
    r"자동\s*요약|핵심\s*요약|요약봇|번역|글자\s*크기|크게\s*보기|"
    r"공유|스크랩|인쇄|댓글|공감|좋아요|본문\s*바로가기|뉴스\s*홈|"
    r"로그인|메뉴|검색|기자\s*구독|언론사\s*구독)(?:\s*\d+)?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_BRACKET_ONLY_PATTERN = re.compile(
    r"^\s*(?:\[[^\]\r\n]{1,120}\]|"
    r"【[^】\r\n]{1,120}】|［[^］\r\n]{1,120}］)"
    r"(?:\s*(?:\[[^\]\r\n]{1,120}\]|"
    r"【[^】\r\n]{1,120}】|［[^］\r\n]{1,120}］))*\s*[.!]?\s*$"
)
ARTICLE_BODY_ANY_BRACKET_PATTERN = re.compile(
    r"(?:\[[^\]\r\n]*\]|【[^】\r\n]*】|［[^］\r\n]*］)"
    r"(?:\s*(?:을|를|은|는|이|가|과|와|에서|으로|로))?"
)
ARTICLE_BODY_EMAIL_PATTERN = re.compile(
    r"(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}(?![A-Z0-9.-])",
    re.IGNORECASE,
)
ARTICLE_BODY_EMAIL_ONLY_PATTERN = re.compile(
    r"^\s*[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_DATETIME_PATTERN_TEXT = (
    r"(?:\d{4}\s*[./-]\s*\d{1,2}\s*[./-]\s*\d{1,2}\.?|"
    r"\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일)"
    r"(?:\s*\([^)]{1,10}\))?"
    r"(?:\s*(?:T|오전|오후|am|pm)?\s*\d{1,2}:\d{2}(?::\d{2})?)?"
)
ARTICLE_BODY_TIMESTAMP_LABEL_PATTERN_TEXT = (
    r"(?:입력(?:일|시간)?|등록(?:일|시간)?|작성(?:일|시간)?|"
    r"수정(?:일|시간)?|최종\s*수정|업데이트|송고(?:일시|시간)?|"
    r"승인(?:일|시간)?|발행(?:일|시간)?|게재(?:일|시간)?|최종\s*편집)"
)
ARTICLE_BODY_LEADING_AUTHOR_TIMESTAMP_PATTERN = re.compile(
    rf"^\s*[\[(]?\s*"
    rf"(?:{ARTICLE_BODY_PERSON_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}?\s+)?"
    rf"(?:기사\s*)?{ARTICLE_BODY_TIMESTAMP_LABEL_PATTERN_TEXT}"
    rf"\s*[:=]?\s*"
    rf"{ARTICLE_BODY_DATETIME_PATTERN_TEXT}"
    rf"(?:\s+{ARTICLE_BODY_TIMESTAMP_LABEL_PATTERN_TEXT}\s*[:=]?\s*"
    rf"{ARTICLE_BODY_DATETIME_PATTERN_TEXT})*"
    rf"\s*[\])]?[|·,;:\-]*\s*",
    re.IGNORECASE,
)
ARTICLE_BODY_REPORTER_LINE_PATTERN = re.compile(
    r"^\s*(?:[\[(][^\])\n]{0,80}[\])]\s*)?"
    r"(?:(?:글|사진|취재|영상)\s*[·:=]?\s*)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:\s*(?:[|=:,\-()]|구독))*\s*"
    r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_REPEATED_REPORTER_LINE_PATTERN = re.compile(
    rf"^\s*{ARTICLE_BODY_PERSON_PATTERN_TEXT}\s*{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    rf"(?:\s*[,·/&]\s*{ARTICLE_BODY_PERSON_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT})+\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_BRACKETED_REPORTER_PATTERN = re.compile(
    r"^\s*[\[(][^\])\n]{0,100}?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}\s*"
    r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\s*)?"
    r"[\])]\s*(?:[=:,\-]\s*)?",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_REPORTER_PATTERN = re.compile(
    r"^\s*(?:[\[(][^\])\n]{0,80}[\])]\s*)?"
    r"(?:(?:글|사진|취재|영상)\s*[·:=]?\s*)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?![가-힣A-Za-z0-9])\s*"
    r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\s*)?"
    r"(?:[=:|\-]\s*)",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_REPORTER_WITHOUT_SEPARATOR_PATTERN = re.compile(
    r"^\s*(?:[\[(][^\])\n]{0,80}[\])]\s*)?"
    r"(?:(?:글|사진|취재|영상)\s*[·:=]?\s*)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?![가-힣A-Za-z0-9])\s*"
    r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\s*)?"
    r"(?=\S)",
    re.IGNORECASE,
)
ARTICLE_BODY_LEADING_ROLE_NAME_PATTERN = re.compile(
    rf"^\s*(?:[\[(]?\s*{ARTICLE_BODY_ROLE_PATTERN_TEXT}\s*[\])]?)"
    rf"\s*{ARTICLE_BODY_PERSON_PATTERN_TEXT}\s*"
    r"(?:[|=:·,\-]\s*|(?=\S))",
    re.IGNORECASE,
)
ARTICLE_BODY_TRAILING_REPORTER_PATTERN = re.compile(
    r"\s+(?:[\[(][^\])\n]{0,80}[\])]\s*)?"
    r"(?:(?:글|사진|취재|영상)\s*[·:=]?\s*)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:\s*(?:[|=:,\-()]|구독))*\s*"
    r"(?:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_TRAILING_REPORTER_SENTENCE_PATTERN = re.compile(
    r"\s+(?:이상\s+)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:였|이)?습니다[.!?]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_DELIMITED_REPORTER_PATTERN = re.compile(
    r"(?:^|\s*[|·]\s*)"
    r"(?:(?:글|사진|취재|영상)\s*[·:=]?\s*)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:\s*[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})?"
    r"(?=\s*(?:[|·=:]+|$))",
    re.IGNORECASE,
)
ARTICLE_BODY_PIPE_BYLINE_SEGMENT_PATTERN = re.compile(
    r"[|｜│┃]\s*"
    rf"(?:{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*|"
    r"[^|｜│┃\r\n]{0,100}?(?:=|뉴스|신문|방송|통신|일보)\s*"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*)"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:\s*[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})?"
    r"\s*[|｜│┃]",
    re.IGNORECASE,
)
ARTICLE_BODY_COPYRIGHT_PATTERN = re.compile(
    r"(?:저작권자?\s*(?:[©ⓒ]|\(c\))|[©ⓒ]|\(c\)|"
    r"copyrights?\b(?!\s+(?:Inc|LLC|Ltd|Corp)\b)|"
    r"all rights reserved|"
    r"무단\s*(?:전재|복제|배포)(?:\s*(?:및|·|/|-)?\s*"
    r"(?:재배포|복제|배포))*\s*(?:금지|를?\s*금합니다)|"
    r"(?:전재|복제|재배포)\s*(?:및|·|/|-)?\s*(?:재배포|배포)?\s*금지|"
    r"(?:AI|인공지능)\s*(?:학습|활용|데이터베이스|DB\s*이용)"
    r"[^.!?。]{0,50}(?:금지|사용할\s*수\s*없))",
    re.IGNORECASE,
)
ARTICLE_BODY_TIP_OR_CONTACT_CTA_PATTERN = re.compile(
    r"(?:기사|뉴스|취재|영상|사진|독자|시청자|24\s*시간)\s*제보\s*[:=]|"
    r"(?:기사|뉴스|취재|영상|사진|독자|시청자|24\s*시간)\s*제보는?"
    r"[^.!?。]{0,80}(?:@|전화|메일|이메일|카카오톡|부탁|주세요|받습니다)|"
    r"(?:기사|뉴스|독자|시청자)\s*제보\s*$|제보\s*하기|"
    r"제보\s*(?:[:=]|전화|메일|이메일|카카오톡|\(?\d)|"
    r"(?:당신|여러분|독자\s*여러분)의?\s*제보|"
    r"제보를?\s*(?:기다립니다|받습니다|주세요|부탁드립니다)|"
    r"(?:광고|보도자료|콘텐츠|기사)\s*(?:문의|접수)"
    r"(?=\s*(?:[:=]|전화|메일|이메일|\(?\d|$))|"
    r"(?:정정|반론)\s*보도\s*(?:신청|청구|문의)|고충처리인|"
    r"기사\s*(?:삭제|수정)\s*(?:요청|문의)|"
    r"(?:문의|대표전화|편집국|독자서비스)\s*[:=]|"
    r"contact\s*[:=]",
    re.IGNORECASE,
)
ARTICLE_BODY_SOCIAL_PLATFORM_PATTERN_TEXT = (
    r"(?:\ub124\uc774\ubc84(?:\s*뉴스)?|다음|카카오톡?|구글\s*뉴스|유튜브|"
    r"인스타그램|페이스북|텔레그램|트위터|언론사\s*홈|"
    r"(?<![A-Za-z0-9])X(?![A-Za-z0-9]))"
)
ARTICLE_BODY_SUBSCRIPTION_CTA_PATTERN = re.compile(
    rf"(?:{ARTICLE_BODY_SOCIAL_PLATFORM_PATTERN_TEXT}[^.!?。]{{0,40}})?"
    r"채널(?:을|에)?\s*"
    r"(?:구독\s*(?:해\s*(?:주세요|주십시오|보세요)|하세요|바랍니다|"
    r"부탁드립니다)|추가\s*(?:해\s*(?:주세요|주십시오)|하세요|"
    r"바랍니다|하고[^.!?。]{0,60}(?:소식|뉴스|콘텐츠|확인|만나)))|"
    r"뉴스레터를?\s*(?:구독|신청)\s*(?:해\s*(?:주세요|주십시오)|"
    r"하세요|바랍니다|하기)|"
    rf"{ARTICLE_BODY_SOCIAL_PLATFORM_PATTERN_TEXT}[^.!?。]{{0,30}}"
    r"(?:뉴스|콘텐츠|매체|언론사)?(?:를|을)?\s*구독\s*"
    r"(?:해\s*(?:주세요|주십시오)|하세요|바랍니다)|"
    r"앱(?:을)?\s*(?:다운로드|설치)\s*(?:해\s*(?:주세요|주십시오|더|"
    r"새로운|다양한|최신|소식|뉴스|콘텐츠|확인|만나|보세요)|하세요|"
    r"바랍니다)|구독과?\s*좋아요",
    re.IGNORECASE,
)
ARTICLE_BODY_SOCIAL_PROMOTION_LINE_PATTERN = re.compile(
    rf"^\s*(?:{ARTICLE_BODY_SOCIAL_PLATFORM_PATTERN_TEXT})"
    rf"(?:\s*[·,/|]\s*{ARTICLE_BODY_SOCIAL_PLATFORM_PATTERN_TEXT})*"
    r"(?:\s+SNS)?\s+(?:팔로우|구독(?:하기)?|좋아요)"
    r"(?:\s*(?:하기|바로가기|부탁드립니다|해주세요))?"
    r"[.!]?\s*$|^\s*(?:구독하기|채널\s*추가|알림\s*설정|앱\s*다운로드)"
    r"[.!]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_SERVICE_NOTICE_PATTERN = re.compile(
    r"(?:기사의?\s*제목과\s*주요\s*문장을?\s*기반으로|"
    r"기사\s*본문을\s*바탕으로)[^.!?。]{0,80}(?:자동|AI)\s*요약|"
    r"(?:자동|AI)\s*요약한\s*결과|"
    r"음성\s*합성\s*기술로?\s*(?:읽어|제작)|"
    r"외부\s*필진[^.!?。]{0,80}(?:편집\s*방향|의견과\s*다를)",
    re.IGNORECASE,
)
ARTICLE_BODY_MEDIA_PROMO_LINE_PATTERN = re.compile(
    r"^\s*(?:(?:대한민국\s*)?24\s*시간\s*(?:뉴스|보도)\s*채널|"
    r"(?:세상|경제|시대|대한민국)을?\s*보는\s*눈|"
    r"(?:경제|세상|시대)를?\s*읽는\s*힘|"
    r"정확하고\s*빠른\s*뉴스|신뢰받는\s*(?:뉴스|미디어))"
    r"[^.!?。\n]{0,60}[.!]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_PUBLISHER_INTRO_PATTERN = re.compile(
    r"^\s*(?:(?:저희|우리|본)\s+)?"
    r"(?:[가-힣A-Za-z0-9&·.\- ]{1,40}\s*)?"
    r"(?:은|는|이|가)?\s*"
    r"(?:(?:대한민국|국내|지역|글로벌|대표|최고의?|선도하는|정론지|"
    r"종합|경제|온라인|인터넷|디지털|전문)\s*){0,5}"
    r"(?:언론사|신문|방송|뉴스\s*채널|미디어|통신사|경제지)"
    r"(?:로서|이며|이고|입니다|이다|를?\s*지향합니다)"
    r"[^.!?。\n]{0,120}[.!]?\s*$|"
    r"^\s*(?:더\s*많은|다양한|최신)\s*(?:뉴스|기사|콘텐츠|소식)(?:는|를)?\s*"
    r"[^.!?。\n]{0,100}(?:홈페이지|채널|앱|사이트)"
    r"[^.!?。\n]{0,60}(?:확인|방문|이용|만나)[^.!?。\n]{0,20}[.!]?\s*$|"
    r"^\s*(?:we\s+are\s+|[A-Z0-9&.\- ]{1,40}\s+is\s+)?"
    r"(?:a\s+|the\s+)?(?:leading\s+|independent\s+|global\s+|local\s+)*"
    r"(?:news\s+organization|news\s+outlet|newspaper|broadcaster|media\s+company)"
    r"[^.!?\n]{0,120}[.!]?\s*$|"
    r"^\s*(?:read|find|discover)\s+more\s+(?:news|stories|articles|content)"
    r"[^.!?\n]{0,100}(?:website|app|channel)[^.!?\n]{0,40}[.!]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_COMMERCIAL_CTA_PATTERN = re.compile(
    r"^\s*(?:지금|오늘|바로)?\s*"
    r"(?:구매|주문|상담|견적|예약|신청|가입|체험|다운로드)"
    r"(?:하기|하러\s*가기|하세요|해\s*보세요|바로가기|문의)"
    r"[^.!?。\n]{0,100}[.!]?\s*$|"
    r"^\s*(?:특가|할인|이벤트|프로모션|무료\s*상담|구매\s*문의)\s*[:=]"
    r"[^\n]{0,160}$|"
    r"^\s*(?:shop|buy|order|book|sign\s*up|download|contact\s+us)\s+now"
    r"[^.!?\n]{0,100}[.!]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_TRAILING_CREDIT_SENTENCE_PATTERN = re.compile(
    r"^(?:이상\s+)?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:(?:였|이)?습니다)?[.!?]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_BROADCAST_SIGNOFF_PATTERN = re.compile(
    rf"^\s*(?:지금까지\s+[^.!?。\n]{{0,30}}?"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"(?:{ARTICLE_BODY_ROLE_PATTERN_TEXT}\s*)?|"
    rf"[^.!?。\n]{{0,20}}(?:뉴스|신문|방송|일보|통신|TV|티브이)\s+"
    rf"{ARTICLE_BODY_BYLINE_NAMES_PATTERN_TEXT}\s*"
    rf"(?:{ARTICLE_BODY_ROLE_PATTERN_TEXT}\s*)?)(?:이었|였|이)?습니다[.!?]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[.!?。])\s+|[\r\n]+")
ARTICLE_BODY_URL_PATTERN = re.compile(
    r"(?:https?://|www\.)\S+",
    re.IGNORECASE,
)
ARTICLE_BODY_URL_ONLY_PATTERN = re.compile(
    r"^\s*(?:(?:https?://|www\.)\S+|"
    r"(?:[A-Z0-9-]+\.)+(?:COM|NET|ORG|KR|CO\.KR|IO|AI)(?:/\S*)?|"
    r"@[A-Z0-9_.-]+)"
    r"(?:\s*[|·,/]\s*(?:(?:https?://|www\.)\S+|"
    r"(?:[A-Z0-9-]+\.)+(?:COM|NET|ORG|KR|CO\.KR|IO|AI)(?:/\S*)?|"
    r"@[A-Z0-9_.-]+))*\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_NAVIGATION_LABEL_PATTERN_TEXT = (
    r"(?:관련\s*(?:기사|뉴스)|추천\s*(?:기사|뉴스)|"
    r"함께\s*(?:읽기|읽은\s*(?:기사|뉴스)|본\s*(?:기사|뉴스)|"
    r"보면\s*좋은\s*(?:기사|뉴스)|읽어\s*볼\s*(?:기사|뉴스))|"
    r"많이\s*(?:본|읽은)\s*(?:기사|뉴스)|인기\s*(?:기사|뉴스)|"
    r"주요\s*뉴스|오늘의\s*(?:주요\s*)?뉴스|핫\s*뉴스|최신\s*뉴스|"
    r"이\s*시각\s*(?:뉴스|추천|주요\s*뉴스)|실시간\s*(?:뉴스|랭킹)|"
    r"이런\s*(?:기사|뉴스)\s*어떠세요|놓치면\s*안\s*될\s*(?:기사|뉴스)|"
    r"다음\s*기사|이전\s*기사|기사\s*목록|더보기|원문\s*보기|기사\s*원문|"
    r"기자\s*(?:프로필|페이지|의\s*다른\s*기사)|이\s*기자의?\s*최신\s*기사|"
    r"언론사\s*(?:홈|홈페이지)|공식\s*(?:홈페이지|SNS))"
)
ARTICLE_BODY_UI_ITEM_PATTERN_TEXT = (
    r"(?:원문\s*보기|기사\s*원문|공유하기|공감(?:\s*\d+)?|"
    r"댓글(?:\s*\d+)?|스크랩|인쇄|글자\s*크기|음성으로\s*듣기|"
    r"자동\s*요약)"
)
ARTICLE_BODY_TRAILING_NAVIGATION_PATTERN = re.compile(
    r"^\s*(?:[▶▷☞※■□◆◇●○★☆+\-]+\s*)?"
    rf"(?:{ARTICLE_BODY_NAVIGATION_LABEL_PATTERN_TEXT}\s*[:=]?|"
    rf"{ARTICLE_BODY_UI_ITEM_PATTERN_TEXT}(?:\s*[|·]\s*"
    rf"{ARTICLE_BODY_UI_ITEM_PATTERN_TEXT})*)\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_TRAILING_DISCLAIMER_PATTERN = re.compile(
    r"(?:^|[※*]\s*)(?:(?:본|이|해당)\s*(?:기사|콘텐츠|자료)는?\s*"
    r"(?:투자\s*(?:권유|판단|참고|책임)|광고|협찬|후원|제작비|유료|"
    r"브랜드|프리미엄|회원\s*전용)|"
    r"(?:이|해당)\s*(?:기사|콘텐츠)는?[^.!?。]{0,80}"
    r"(?:협찬|후원|제작비)[^.!?。]{0,30}(?:받|지원)|"
    r"투자\s*권유를?\s*목적으로?|투자\s*판단의?\s*책임은?\s*"
    r"투자자\s*본인|자료\s*제공\s*[:=]|보도자료\s*제공|"
    r"(?:광고주|기업|기관)(?:의|으로부터|가)?[^.!?。]{0,60}"
    r"(?:(?:협찬|후원)을?\s*받|제작비를?\s*지원)|"
    r"(?:제품|서비스|원고료|경제적\s*대가|제작비)를?[^.!?。]{0,30}"
    r"(?:제공|지원)\s*받|"
    r"파트너스[^.!?。]{0,50}수수료|"
    r"(?:프리미엄|유료|회원\s*전용)\s*(?:콘텐츠|기사|서비스)[^.!?。]{0,60}"
    r"(?:구독|가입|로그인|제공|게재))",
    re.IGNORECASE,
)
ARTICLE_BODY_PRODUCTION_CREDIT_LABEL_PATTERN_TEXT = (
    r"(?:영상\s*(?:취재|편집|제작|제공)|취재(?:기자)?|촬영(?:기자)?|"
    r"카메라|그래픽|CG|디자인|편집|연출|구성|진행|내레이션|글|"
    r"사진\s*(?:제공|취재)?|사진제공|자료\s*사진|화면\s*(?:제공|캡처)|"
    r"화면제공|자료\s*제공|음원\s*제공)"
)
ARTICLE_BODY_PRODUCTION_CREDIT_PATTERN = re.compile(
    r"^\s*(?:[▶▷☞※■□◆◇●○★☆+\-]+\s*)?(?:[\[(]\s*)?"
    rf"{ARTICLE_BODY_PRODUCTION_CREDIT_LABEL_PATTERN_TEXT}\s*[:=·]\s*"
    r"[^.!?。\n|·/]{1,80}"
    rf"(?:\s*[|·/]\s*{ARTICLE_BODY_PRODUCTION_CREDIT_LABEL_PATTERN_TEXT}"
    r"\s*[:=·]\s*[^.!?。\n|·/]{1,80})*"
    r"\s*(?:[\])])?\s*[.]?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_TIMESTAMP_LINE_PATTERN = re.compile(
    rf"^\s*(?:{ARTICLE_BODY_TIMESTAMP_LABEL_PATTERN_TEXT}\s*[:=]?\s*"
    rf"{ARTICLE_BODY_DATETIME_PATTERN_TEXT}"
    rf"\s*)+$",
    re.IGNORECASE,
)
ARTICLE_BODY_TAG_LINE_PATTERN = re.compile(
    r"^\s*(?:(?:태그|키워드|해시태그)\s*[:=]\s*.+|(?:#\S+\s*)+)$",
    re.IGNORECASE,
)
ARTICLE_BODY_DECORATION_LINE_PATTERN = re.compile(r"^\s*[-_=·•◆◇■□▶▷※★☆|/\\]{3,}\s*$")
ARTICLE_BODY_PHONE_PATTERN = re.compile(r"(?<!\d)(?:0\d{1,2}[-.)\s]?)?\d{3,4}[-\s]\d{4}(?!\d)")
ARTICLE_BODY_ROLE_AT_END_PATTERN = re.compile(
    rf"{ARTICLE_BODY_ROLE_PATTERN_TEXT}"
    r"(?:\s*[|·,;:\-])?\s*$",
    re.IGNORECASE,
)
ARTICLE_BODY_STRONG_FOOTER_START_PATTERN = re.compile(
    r"^\s*(?:[▶▷☞※■□◆◇●○★☆+\-]+\s*)?(?:"
    rf"{ARTICLE_BODY_NAVIGATION_LABEL_PATTERN_TEXT}\s*[:=]?|"
    r"(?:태그|키워드|해시태그)\s*[:=]\s*)$",
    re.IGNORECASE,
)
ARTICLE_BODY_REPORTED_CONTEXT_PATTERN = re.compile(
    r"(?:에\s*따르면|라고\s*(?:밝혔|말했|설명했|강조했)|"
    r"(?:밝혔다|말했다|설명했다|강조했다|발표했다|재확인했다)|"
    r"(?:당국|정부|회사|기업|관계자|전문가)는?\s*['\"“”‘’]|['\"“”‘’])",
    re.IGNORECASE,
)
ARTICLE_BODY_MEANINGFUL_CONTENT_PATTERN = re.compile(
    r"(?:계약|공급|수주|매출|영업이익|순이익|투자|인수|합병|"
    r"생산|양산|출시|승인|허가|규제|소송|실적|수요|가격|"
    r"증설|감산|정책|정부|회사|기업)",
    re.IGNORECASE,
)


def calculate_trailing_noise_score(block: str) -> tuple[int, list[str]]:

    block = re.sub(r"\s+", " ", block or "").strip()

    if not block:
        return 10, ["empty"]

    score = 0
    reasons = []
    has_reported_context = bool(ARTICLE_BODY_REPORTED_CONTEXT_PATTERN.search(block))

    if ARTICLE_BODY_COPYRIGHT_PATTERN.search(block) and not has_reported_context:
        score += 6
        reasons.append("copyright_notice")

    if ARTICLE_BODY_TIP_OR_CONTACT_CTA_PATTERN.search(block) and not has_reported_context:
        score += 6
        reasons.append("tip_or_contact_cta")

    if ARTICLE_BODY_SUBSCRIPTION_CTA_PATTERN.search(block) and not has_reported_context:
        score += 6
        reasons.append("subscription_cta")

    if ARTICLE_BODY_SOCIAL_PROMOTION_LINE_PATTERN.fullmatch(block):
        score += 6
        reasons.append("social_promotion")

    if ARTICLE_BODY_SERVICE_NOTICE_PATTERN.search(block) and not has_reported_context:
        score += 6
        reasons.append("service_notice")

    if ARTICLE_BODY_MEDIA_PROMO_LINE_PATTERN.fullmatch(block):
        score += 6
        reasons.append("media_promotion")

    if ARTICLE_BODY_PUBLISHER_INTRO_PATTERN.fullmatch(block):
        score += 7
        reasons.append("publisher_introduction")

    if ARTICLE_BODY_COMMERCIAL_CTA_PATTERN.fullmatch(block):
        score += 7
        reasons.append("commercial_cta")

    if ARTICLE_BODY_TRAILING_NAVIGATION_PATTERN.search(block):
        score += 5
        reasons.append("navigation_or_social")

    if ARTICLE_BODY_TRAILING_DISCLAIMER_PATTERN.search(block) and not has_reported_context:
        score += 6
        reasons.append("investment_or_source_disclaimer")

    if ARTICLE_BODY_PRODUCTION_CREDIT_PATTERN.fullmatch(block) and not has_reported_context:
        score += 7
        reasons.append("production_credit")

    if ARTICLE_BODY_TIMESTAMP_LINE_PATTERN.fullmatch(block):
        score += 7
        reasons.append("publication_timestamp")

    if ARTICLE_BODY_TAG_LINE_PATTERN.fullmatch(block):
        score += 5
        reasons.append("tag_metadata")

    if ARTICLE_BODY_DECORATION_LINE_PATTERN.fullmatch(block):
        score += 5
        reasons.append("decorative_separator")

    if (
        ARTICLE_BODY_TRAILING_CREDIT_SENTENCE_PATTERN.fullmatch(block)
        or ARTICLE_BODY_BROADCAST_SIGNOFF_PATTERN.fullmatch(block)
        or ARTICLE_BODY_REPEATED_REPORTER_LINE_PATTERN.fullmatch(block)
    ):
        score += 7
        reasons.append("reporter_credit")
    elif len(block) <= 200 and ARTICLE_BODY_ROLE_AT_END_PATTERN.search(block):
        score += 4
        reasons.append("short_byline")

    has_contact_signal = bool(
        ARTICLE_BODY_TIP_OR_CONTACT_CTA_PATTERN.search(block) and not has_reported_context
    )

    if ARTICLE_BODY_EMAIL_PATTERN.search(block):
        score += 3
        reasons.append("email")

        if has_contact_signal:
            score += 3
            reasons.append("email_with_contact_signal")

    if ARTICLE_BODY_EMAIL_ONLY_PATTERN.fullmatch(block):
        score += 5
        reasons.append("standalone_email")

    if ARTICLE_BODY_PHONE_PATTERN.search(block) and has_contact_signal:
        score += 6
        reasons.append("contact_phone")

    if ARTICLE_BODY_URL_PATTERN.search(block):
        score += 2
        reasons.append("url")

        if has_contact_signal or ARTICLE_BODY_TRAILING_NAVIGATION_PATTERN.search(block):
            score += 2
            reasons.append("promotional_url")

    if ARTICLE_BODY_URL_ONLY_PATTERN.fullmatch(block):
        score += 5
        reasons.append("standalone_url_or_social_handle")

    if ARTICLE_BODY_AD_LINE_PATTERN.fullmatch(block):
        score += 7
        reasons.append("advertisement")

    if ARTICLE_BODY_MEDIA_LABEL_LINE_PATTERN.fullmatch(block):
        score += 7
        reasons.append("standalone_media_label")

    if ARTICLE_BODY_COMPACT_MEDIA_CREDIT_LINE_PATTERN.fullmatch(block):
        score += 8
        reasons.append("compact_media_credit")

    if ARTICLE_BODY_BRACKET_ONLY_PATTERN.fullmatch(block):
        score += 6
        reasons.append("standalone_bracket_metadata")

    strong_reasons = {
        "copyright_notice",
        "tip_or_contact_cta",
        "subscription_cta",
        "social_promotion",
        "service_notice",
        "media_promotion",
        "publisher_introduction",
        "commercial_cta",
        "navigation_or_social",
        "investment_or_source_disclaimer",
        "production_credit",
        "publication_timestamp",
        "tag_metadata",
        "decorative_separator",
        "reporter_credit",
        "email_with_contact_signal",
        "standalone_email",
        "contact_phone",
        "promotional_url",
        "standalone_url_or_social_handle",
        "advertisement",
        "standalone_media_label",
        "compact_media_credit",
        "standalone_bracket_metadata",
    }

    if not strong_reasons.intersection(reasons):
        if ARTICLE_BODY_MEANINGFUL_CONTENT_PATTERN.search(block):
            score = max(0, score - 3)
            reasons.append("meaningful_content_guard")
        else:
            score = min(score, 4)
            reasons.append("weak_signal_guard")

    return score, reasons


def calculate_leading_noise_score(block: str) -> tuple[int, list[str]]:

    block = re.sub(r"\s+", " ", block or "").strip()

    if not block:
        return 10, ["empty"]

    trailing_score, trailing_reasons = calculate_trailing_noise_score(block)

    if trailing_score >= 5:
        return trailing_score, trailing_reasons

    score = 0
    reasons = []

    if ARTICLE_BODY_LEADING_IMAGE_CAPTION_PATTERN.fullmatch(block):
        score += 7
        reasons.append("leading_image_caption_or_credit")

    if ARTICLE_BODY_LOCATION_SOURCE_LINE_PATTERN.fullmatch(block):
        score += 7
        reasons.append("leading_location_source")

    if ARTICLE_BODY_BARE_MEDIA_NAME_PATTERN.fullmatch(block):
        score += 6
        reasons.append("leading_bare_media_name")

    if ARTICLE_BODY_STANDALONE_SOURCE_CREDIT_PATTERN.fullmatch(block):
        score += 7
        reasons.append("leading_source_credit")

    if ARTICLE_BODY_READER_TOOL_PATTERN.fullmatch(block):
        score += 7
        reasons.append("leading_reader_tool")

    return score, reasons


def remove_leading_noise_blocks(
    blocks: list[str],
    max_blocks: int = 30,
    threshold: int = 5,
) -> tuple[list[str], list[dict[str, Any]]]:

    kept_blocks = list(blocks)
    removed_blocks = []
    removed_count = 0

    while kept_blocks and removed_count < max_blocks:
        candidate = kept_blocks[0]
        score, reasons = calculate_leading_noise_score(candidate)

        if score < threshold:
            break

        kept_blocks.pop(0)
        removed_blocks.append(
            {
                "text": candidate,
                "score": score,
                "reasons": reasons,
            }
        )
        removed_count += 1

    return kept_blocks, removed_blocks


def remove_leading_repeated_title(
    blocks: list[str],
    article_title: str,
) -> tuple[list[str], list[dict[str, Any]]]:

    normalized_title = clean_text(ARTICLE_BODY_ANY_BRACKET_PATTERN.sub(" ", article_title or ""))

    if len(normalized_title) < 8:
        return list(blocks), []

    kept_blocks = list(blocks)
    removed_blocks = []

    while kept_blocks and len(removed_blocks) < 2:
        normalized_block = clean_text(kept_blocks[0])

        if normalized_block != normalized_title:
            break

        removed_blocks.append(
            {
                "text": kept_blocks.pop(0),
                "score": 7,
                "reasons": ["repeated_article_title"],
            }
        )

    return kept_blocks, removed_blocks


def remove_trailing_noise_blocks(
    blocks: list[str],
    max_blocks: int | None = None,
    threshold: int = 5,
) -> tuple[list[str], list[dict[str, Any]]]:

    kept_blocks = list(blocks)
    removed_groups = []

    while kept_blocks and (max_blocks is None or len(removed_groups) < max_blocks):
        candidate = kept_blocks[-1]
        cleaned_candidate, removed_sentences = remove_trailing_noise_sentences_with_details(
            candidate
        )

        if removed_sentences:
            removed_groups.append(removed_sentences)

            if cleaned_candidate:
                kept_blocks[-1] = cleaned_candidate
                break

            kept_blocks.pop()
            continue

        score, reasons = calculate_trailing_noise_score(candidate)

        if score < threshold:
            break

        kept_blocks.pop()
        removed_groups.append(
            [
                {
                    "text": candidate,
                    "score": score,
                    "reasons": reasons,
                }
            ]
        )

    removed_blocks = [removed for group in reversed(removed_groups) for removed in group]

    return kept_blocks, removed_blocks


def remove_trailing_noise_region(
    blocks: list[str],
    max_tail_blocks: int = 30,
    max_tail_chars: int = 6000,
) -> tuple[list[str], list[dict[str, Any]]]:

    if not blocks:
        return [], []

    minimum_index = max(0, len(blocks) - max_tail_blocks)
    tail_block_start = len(blocks) - 1
    tail_chars = 0

    for index in range(len(blocks) - 1, minimum_index - 1, -1):
        block_length = len(blocks[index])

        if tail_chars and tail_chars + block_length > max_tail_chars:
            break

        tail_block_start = index
        tail_chars += block_length

        if tail_chars >= max_tail_chars:
            break

    for index in range(tail_block_start, len(blocks)):
        block = blocks[index]
        search_start = 0

        if index == tail_block_start and len(block) > max_tail_chars:
            search_start = len(block) - max_tail_chars

        match = ARTICLE_BODY_STRONG_FOOTER_START_PATTERN.search(
            block,
            search_start,
        )

        if not match:
            continue

        prefix = block[: match.start()].rstrip()
        prefix = re.sub(
            r"[\s|·•◆◇■□▶▷※★☆+\-]+$",
            "",
            prefix,
        ).rstrip()
        kept_blocks = list(blocks[:index])

        if prefix:
            kept_blocks.append(prefix)

        removed_blocks = []
        matched_suffix = block[match.start() :].strip()

        if matched_suffix:
            removed_blocks.append(
                {
                    "text": matched_suffix,
                    "score": 10,
                    "reasons": ["footer_region_start"],
                }
            )

        for trailing_block in blocks[index + 1 :]:
            removed_blocks.append(
                {
                    "text": trailing_block,
                    "score": 10,
                    "reasons": ["after_footer_region_start"],
                }
            )

        return kept_blocks, removed_blocks

    return list(blocks), []


def is_trailing_noise_sentence(sentence: str) -> bool:

    sentence = re.sub(r"\s+", " ", sentence or "").strip()

    if not sentence:
        return True

    score, _ = calculate_trailing_noise_score(sentence)
    return score >= 5


def remove_trailing_noise_sentences_with_details(
    text: str,
    max_sentences: int | None = None,
) -> tuple[str, list[dict[str, Any]]]:

    if not text:
        return "", []

    sentences = [
        sentence.strip()
        for sentence in ARTICLE_BODY_SENTENCE_BOUNDARY_PATTERN.split(text)
        if sentence.strip()
    ]
    removed_count = 0
    removed_sentences = []

    while (
        sentences
        and (max_sentences is None or removed_count < max_sentences)
        and is_trailing_noise_sentence(sentences[-1])
    ):
        removed_sentence = sentences.pop()
        score, reasons = calculate_trailing_noise_score(removed_sentence)
        removed_sentences.append(
            {
                "text": removed_sentence,
                "score": score,
                "reasons": reasons,
            }
        )
        removed_count += 1

    removed_sentences.reverse()

    return " ".join(sentences).strip(), removed_sentences


def append_noise_change(
    removed_noise: list[dict[str, Any]] | None,
    before: str,
    after: str,
    reason: str,
    score: int = 6,
) -> None:

    if removed_noise is None or before == after:
        return

    removed_noise.append(
        {
            "text": before,
            "cleaned_text": after,
            "score": score,
            "reasons": [reason],
        }
    )


def clean_text(text: str) -> str:
    if not text:
        return ""

    text = html.unescape(text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"[^가-힣a-zA-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text.lower()


def get_printable_text(text: str) -> str:
    if not text:
        return ""

    text = html.unescape(text)
    text = re.sub(r"<.*?>", "", text)

    return text.strip()


TITLE_LEADING_BRACKET_PATTERN = re.compile(r"^(?:\s*\[[^\]]*\])+\s*")


def remove_leading_title_brackets(title: str) -> str:
    """제목 선두의 "[속보]" 같은 브라켓 태그들을 제거한다. 저장 전 제목 정리용."""

    if not title:
        return ""

    return TITLE_LEADING_BRACKET_PATTERN.sub("", title).strip()


def clean_article_body_for_storage(
    text: str,
    removed_noise: list[dict[str, Any]] | None = None,
    article_title: str = "",
) -> str:

    if not text:
        return ""

    text = html.unescape(str(text))
    text = ARTICLE_BODY_HTML_BREAK_PATTERN.sub("\n", text)
    text = re.sub(r"<.*?>", "", text)
    text = text.translate(ARTICLE_BODY_TRANSLATION_TABLE)

    cleaned_lines = []

    for raw_line in re.split(r"[\r\n]+", text):
        line = re.sub(r"\s+", " ", raw_line).strip()
        before = line
        line = ARTICLE_BODY_LEADING_AUTHOR_TIMESTAMP_PATTERN.sub(
            "",
            line,
            count=1,
        ).strip()
        append_noise_change(
            removed_noise,
            before,
            line,
            "leading_author_timestamp",
        )

        before = line
        line = ARTICLE_BODY_LEADING_LOCATION_SOURCE_PATTERN.sub(
            "",
            line,
            count=1,
        ).strip()
        append_noise_change(
            removed_noise,
            before,
            line,
            "leading_location_source",
        )

        before = line
        line = ARTICLE_BODY_PIPE_BYLINE_SEGMENT_PATTERN.sub(" ", line).strip()
        append_noise_change(
            removed_noise,
            before,
            line,
            "pipe_byline_segment",
        )

        before = line
        line = ARTICLE_BODY_LEADING_MEDIA_CREDIT_PATTERN.sub(
            "",
            line,
            count=1,
        ).strip()
        append_noise_change(
            removed_noise,
            before,
            line,
            "leading_media_credit",
        )

        if not line:
            continue

        if ARTICLE_BODY_AD_LINE_PATTERN.fullmatch(line):
            append_noise_change(
                removed_noise,
                line,
                "",
                "advertisement_line",
                score=7,
            )
            continue

        if ARTICLE_BODY_REPORTER_LINE_PATTERN.fullmatch(
            line
        ) or ARTICLE_BODY_REPEATED_REPORTER_LINE_PATTERN.fullmatch(line):
            append_noise_change(
                removed_noise,
                line,
                "",
                "reporter_credit_line",
                score=7,
            )
            continue

        before = line
        line = ARTICLE_BODY_LEADING_ROLE_NAME_PATTERN.sub(
            "",
            line,
            count=1,
        ).strip()
        line = ARTICLE_BODY_BRACKETED_REPORTER_PATTERN.sub("", line).strip()
        line = ARTICLE_BODY_LEADING_REPORTER_PATTERN.sub("", line).strip()
        line = ARTICLE_BODY_LEADING_REPORTER_WITHOUT_SEPARATOR_PATTERN.sub(
            "",
            line,
            count=1,
        ).strip()
        line = ARTICLE_BODY_DELIMITED_REPORTER_PATTERN.sub(" ", line).strip()
        append_noise_change(
            removed_noise,
            before,
            line,
            "reporter_or_anchor_byline",
        )

        before = line
        line = ARTICLE_BODY_ANY_BRACKET_PATTERN.sub(" ", line).strip()
        line = re.sub(r"\s+", " ", line)
        append_noise_change(
            removed_noise,
            before,
            line,
            "bracketed_text",
        )

        if not line:
            continue

        if ARTICLE_BODY_STRONG_FOOTER_START_PATTERN.search(line):
            cleaned_lines.append(line)
            continue

        line, removed_inline_sentences = remove_trailing_noise_sentences_with_details(line)

        if removed_noise is not None:
            removed_noise.extend(removed_inline_sentences)

        if not line:
            continue

        noise_score, noise_reasons = calculate_trailing_noise_score(line)

        if noise_score >= 5:
            if removed_noise is not None:
                removed_noise.append(
                    {
                        "text": line,
                        "score": noise_score,
                        "reasons": noise_reasons,
                    }
                )
            continue

        cleaned_lines.append(line)

    cleaned_lines, removed_leading = remove_leading_noise_blocks(cleaned_lines)

    if removed_noise is not None:
        removed_noise.extend(removed_leading)

    cleaned_lines, removed_titles = remove_leading_repeated_title(
        blocks=cleaned_lines,
        article_title=article_title,
    )

    if removed_noise is not None:
        removed_noise.extend(removed_titles)

    cleaned_lines, removed_after_title = remove_leading_noise_blocks(cleaned_lines)

    if removed_noise is not None:
        removed_noise.extend(removed_after_title)

    cleaned_lines, removed_region = remove_trailing_noise_region(cleaned_lines)

    if removed_noise is not None:
        removed_noise.extend(removed_region)

    cleaned_lines, removed_blocks = remove_trailing_noise_blocks(cleaned_lines)

    if removed_noise is not None:
        removed_noise.extend(removed_blocks)

    cleaned_text = "\n".join(cleaned_lines)

    before = cleaned_text
    cleaned_text = ARTICLE_BODY_TRAILING_REPORTER_PATTERN.sub("", cleaned_text)
    cleaned_text = ARTICLE_BODY_TRAILING_REPORTER_SENTENCE_PATTERN.sub(
        "",
        cleaned_text,
    )
    append_noise_change(
        removed_noise,
        before,
        cleaned_text,
        "trailing_reporter_credit",
    )

    cleaned_text = re.sub(r"[ \t]+", " ", cleaned_text)
    cleaned_text = re.sub(r"\s*\n\s*", "\n", cleaned_text)
    cleaned_text = re.sub(r"\n{3,}", "\n\n", cleaned_text).strip()

    return cleaned_text
