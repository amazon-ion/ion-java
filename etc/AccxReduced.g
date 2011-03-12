grammar AccxReduced;

options {
    k=2;
    output=AST;
} 

@header {
package amazon.accxreduced;
}
@lexer::header {
package amazon.accxreduced;
}

datagram
    :	( value )*
    ;
    
container
	:	'(' ( value  (     value  )*        )? ')'
    | 	'[' ( value  ( ',' value  )* (',')? )? ']'
	|	'{' ( member ( ',' member )* (',')? )? '}'
    ;
    
member
	: Identifier ':'        value
	| IdentifierLiteral ':' value
	| StringLiteral ':'     value
	;
	
value
	: ( Identifier '::')        => annotation+ plainvalue
	| ( IdentifierLiteral '::') => annotation+ plainvalue
	|                              plainvalue
	;
	
annotation
	: Identifier '::' 
	| IdentifierLiteral '::'
	;
	
plainvalue
    :  container
    | 'null' ( '.' typename )?
    | 'true'
    | 'false'
    |  Integer
    |  Float
    |  Decimal
    |  Timestamp
    |  Identifier
	|  IdentifierLiteral
    |  StringLiteral
    |  TextLiteral
    |  BinaryLiteral
    ;
    
typename
	: 'null'
	| 'boolean'
	| 'integer'
	| 'float'
	| 'decimal'
	| 'timestamp'
	| 'symbol'
	| 'string'
	| 'clob'
	| 'blob'
	| 'list'
	| 'struct'
	| 'sexp'
	;

// LEXER
  
BinaryLiteral
    : '{{' ( Base64Character )* ( '=' ( '=' )? )? '}}'
    ;

TextLiteral
    : '{{' StringLiteral '}}'
    ;

Identifier
	: Letter ( Letter | Digit )*
	;

IdentifierLiteral
	: '\'' (IdentifierCharacter)* '\''
	;

StringLiteral
	: '"'  (StringCharacter)* '"'
	| (TripleQuote (LongStringCharacter)* TripleQuote)+
	;

fragment
IdentifierCharacter
	: '\t'
	| ('\u0020'..'\u0026') 
	| ('\u0028'..'\u007E')
	| UnicodeCharacter
	| '\\' '"'
	;

fragment
StringCharacter
	: '\t'
	| ('\u0020'..'\u0021') 
	| ('\u0023'..'\u007E')
	| UnicodeCharacter
	| '\\' '"'
	;

fragment
LongStringCharacter
	: '\t'
	| '\u000a'
	| '\u000d'
	| ('\u0020'..'\u0026') 
	| ('\u0028'..'\u007E')
	| UnicodeCharacter
	| '\\' '\''
	;

fragment	
UnicodeCharacter
	: '\u00c2'..'\u00df' '\u0080'..'\u00bf'
	| '\u00E0'..'\u00EF' '\u0080'..'\u00bf' '\u0080'..'\u00bf'
	| '\u00F0'..'\u00F4' '\u0080'..'\u00bf' '\u0080'..'\u00bf' '\u0080'..'\u00bf' 
	;
	
fragment
TripleQuote
	: '\'\'\''
	;

fragment
Base64Character
	: TrueLetter
	| Digit
    | '-'
    | '_'
    ;
    
fragment
TrueLetter
	: 'a'..'z' 
	| 'A'..'Z' 
	;
	
fragment
Letter	
	: TrueLetter
	| '_' 
	| '$' 
	;

fragment
Zero	
    : '0' 
    ;

fragment
NonZeroDigit
	: '1'..'9';

fragment
Digit	
	: Zero
	| NonZeroDigit ;

Integer
	: Zero
	| NonZeroDigit Digit*
	;

fragment
Fraction
	: Digit* ;

Decimal
	: Integer '.' Fraction
	| Integer ( '.' Fraction )? ( 'd' | 'D' ) ( '-' | '+' )? Fraction
	;

Float
	: Integer ('.' Fraction)?  ( 'e' | 'E' ) ('-' | '+')? Fraction 
	;

Timestamp
	:  Year '-' Month '-' Day ( 'T' Hours ':' Minutes ( ':' Seconds ( '.' Fraction )? )? Timezone )?
	;

fragment
Month
	: NonZeroDigit
	| Zero NonZeroDigit
	| '1' ( '0' | '1' | '2')
	;

fragment
Day
	: NonZeroDigit
	| Zero NonZeroDigit
	| NonZeroDigit NonZeroDigit
	;

fragment
Year
	: Digit Digit Digit Digit
	;

fragment
Hours
    :  ( '0' | '1' | '2' )? NonZeroDigit
    ;

fragment
Minutes
    :  ( '0'..'6' )? NonZeroDigit
    ;

fragment
Seconds
    :  ( '0'..'6' )? NonZeroDigit
    ;

fragment
Timezone
    :  ( '+' | '-' ) (Digit)? Digit ':' Digit Digit
    |  'z'
    |  'Z'
    ;
    
WhiteSpace
	: WS
	| COMMENT
	| LINE_COMMENT
	;

fragment
WS  :  (' '|'\r'|'\t'|'\u000C'|'\n') { skip(); }
    ;

fragment
COMMENT
    :   '/*' .* '*/' {  skip();  }
    ;

fragment
LINE_COMMENT
    : '//' ~('\n'|'\r')* '\r'? '\n' {  skip();  }
    ;

