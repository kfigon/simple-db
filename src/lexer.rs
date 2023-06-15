use std::collections::HashMap;

#[allow(dead_code)]

#[derive(Debug, PartialEq, Clone)]
enum Token {
    Keyword(Keyword),
    Identifier(String),
    Number(i32),
    Operator(Operator),
}

#[derive(Debug, PartialEq,Clone)]
enum Keyword {
    Select,
    From,
    Where,
    Join,
    Left,
    Right,
    Group,
    Having,
    Order,
}

#[derive(Debug, PartialEq, Clone)]
enum Operator {
    Plus,
    Minus,
    Star,
    Divide,
    Dot,
    Opening,
    Closing,
    Comma,
    Semicolon
}

#[derive(Debug, PartialEq)]
struct LexerError(String);

fn lex(input: &str) -> Result<Vec<Token>, LexerError> {
    let mut out: Vec<Token> = Vec::new();
    let mut iter = input.chars().peekable();

    let operators: HashMap<char, Operator> = HashMap::from([
        ('+', Operator::Plus),
        ('-', Operator::Minus),
        ('*', Operator::Star),
        ('/', Operator::Divide),
        ('.', Operator::Dot),
        ('(', Operator::Opening),
        (')', Operator::Closing),
        (',', Operator::Comma),
        (';', Operator::Semicolon),
    ]);

    let keywords: HashMap<String, Keyword> = HashMap::from([
        ("select".to_string(), Keyword::Select),
        ("from".to_string(), Keyword::From),
        ("where".to_string(), Keyword::Where),
        ("join".to_string(), Keyword::Join),
        ("left".to_string(), Keyword::Left),
        ("right".to_string(), Keyword::Right),
        ("group".to_string(), Keyword::Group),
        ("having".to_string(), Keyword::Having),
        ("order".to_string(), Keyword::Order),
    ]);

    while let Some(c) = iter.next() {
        if c.is_whitespace() {
            continue;
        } else if c.is_digit(10) {
            let mut dig = String::new();
            dig.push(c);
            while let Some(num_c) = iter.peek() {
                if num_c.is_digit(10) {
                    dig.push(*num_c);
                    iter.next();
                } else {
                    break;
                }
            }
            let num = dig.parse::<i32>().map_err(|_| LexerError(format!("cant parse number {dig}")))?;
            out.push(Token::Number(num));
        } else if let Some(op) = operators.get(&c) {
            out.push(Token::Operator(op.clone()))
        } else {
            let mut word = String::new();
            word.push(c);
            while let Some(word_c) = iter.peek() {
                if word_c.is_alphanumeric() {
                    word.push(*word_c);
                    iter.next();
                } else {
                    break;
                }
            }

            if let Some(keyword) = keywords.get(&word.to_lowercase()) {
                out.push(Token::Keyword(keyword.clone()));
            } else {
                out.push(Token::Identifier(word));
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod test{
    use super::*;

    #[test]
    fn select_1() {
        assert_eq!(lex("select * from mytable;").unwrap(), vec![
            Token::Keyword(Keyword::Select),
            Token::Operator(Operator::Star),
            Token::Keyword(Keyword::From),
            Token::Identifier("mytable".to_owned()),
            Token::Operator(Operator::Semicolon),
        ]);
    }

    #[test]
    fn select_2() {
        assert_eq!(lex("select 123,4 from foo;").unwrap(), vec![
            Token::Keyword(Keyword::Select),
            Token::Number(123),
            Token::Operator(Operator::Comma),
            Token::Number(4),
            Token::Keyword(Keyword::From),
            Token::Identifier("foo".to_owned()),
            Token::Operator(Operator::Semicolon),
        ]);
    }

    #[test]
    fn random_stream_of_tokens() {
        assert_eq!(lex(";;,*   (- +//)
        select, 123.5, ffrom where join GROUP ORDER 
        ").unwrap(), vec![
            Token::Operator(Operator::Semicolon),
            Token::Operator(Operator::Semicolon),
            Token::Operator(Operator::Comma),
            Token::Operator(Operator::Star),
            Token::Operator(Operator::Opening),
            Token::Operator(Operator::Minus),
            Token::Operator(Operator::Plus),
            Token::Operator(Operator::Divide),
            Token::Operator(Operator::Divide),
            Token::Operator(Operator::Closing),
            
            Token::Keyword(Keyword::Select),
            Token::Operator(Operator::Comma),
            Token::Number(123),
            Token::Operator(Operator::Dot),
            Token::Number(5),
            Token::Operator(Operator::Comma),
            Token::Identifier("ffrom".to_owned()),
            Token::Keyword(Keyword::Where),
            Token::Keyword(Keyword::Join),
            Token::Keyword(Keyword::Group),
            Token::Keyword(Keyword::Order),
        ]);
    }
}