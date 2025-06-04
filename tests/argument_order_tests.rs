// tests/simple_order_test.rs
use clap::{ArgAction, ArgMatches, CommandFactory, Parser};

#[derive(Parser)]
#[command(name = "test")]
struct TestArgs {
    #[arg(long = "eval", action = ArgAction::Append)]
    evals: Vec<String>,

    #[arg(long = "filter", action = ArgAction::Append)]
    filters: Vec<String>,
}

#[derive(Debug)]
enum Step {
    Eval(String),
    Filter(String),
}

fn get_ordered_steps(matches: &ArgMatches) -> Vec<Step> {
    let mut steps_with_indices = Vec::new();

    // Get eval steps with their indices
    if let Some(eval_indices) = matches.indices_of("evals") {
        // Use field name, not "eval"
        let eval_values: Vec<&String> = matches
            .get_many::<String>("evals") // Use field name
            .unwrap_or_default()
            .collect();

        for (i, index) in eval_indices.enumerate() {
            if i < eval_values.len() {
                steps_with_indices.push((index, Step::Eval(eval_values[i].clone())));
            }
        }
    }

    // Get filter steps with their indices
    if let Some(filter_indices) = matches.indices_of("filters") {
        // Use field name, not "filter"
        let filter_values: Vec<&String> = matches
            .get_many::<String>("filters") // Use field name
            .unwrap_or_default()
            .collect();

        for (i, index) in filter_indices.enumerate() {
            if i < filter_values.len() {
                steps_with_indices.push((index, Step::Filter(filter_values[i].clone())));
            }
        }
    }

    // Sort by command line position
    steps_with_indices.sort_by_key(|(index, _)| *index);
    steps_with_indices
        .into_iter()
        .map(|(_, step)| step)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indices_of_basic() {
        // Test basic functionality
        let args = vec![
            "test", "--eval", "first", "--filter", "middle", "--eval", "last",
        ];

        let matches = TestArgs::command().try_get_matches_from(args).unwrap();
        let steps = get_ordered_steps(&matches);

        println!("Ordered steps: {:?}", steps);

        // Should be in order: eval(first), filter(middle), eval(last)
        assert_eq!(steps.len(), 3);

        match &steps[0] {
            Step::Eval(s) => assert_eq!(s, "first"),
            _ => panic!("Expected first step to be eval"),
        }

        match &steps[1] {
            Step::Filter(s) => assert_eq!(s, "middle"),
            _ => panic!("Expected second step to be filter"),
        }

        match &steps[2] {
            Step::Eval(s) => assert_eq!(s, "last"),
            _ => panic!("Expected third step to be eval"),
        }

        println!("✅ Basic order test passed");
    }

    #[test]
    fn test_indices_of_multiple_same_type() {
        // Test multiple of same type
        let args = vec![
            "test",
            "--eval",
            "first_eval",
            "--eval",
            "second_eval",
            "--filter",
            "first_filter",
            "--eval",
            "third_eval",
        ];

        let matches = TestArgs::command().try_get_matches_from(args).unwrap();
        let steps = get_ordered_steps(&matches);

        println!("Multiple same type steps: {:?}", steps);

        assert_eq!(steps.len(), 4);

        // Should be: eval, eval, filter, eval
        match &steps[0] {
            Step::Eval(s) => assert_eq!(s, "first_eval"),
            _ => panic!("Expected first step to be eval"),
        }

        match &steps[1] {
            Step::Eval(s) => assert_eq!(s, "second_eval"),
            _ => panic!("Expected second step to be eval"),
        }

        match &steps[2] {
            Step::Filter(s) => assert_eq!(s, "first_filter"),
            _ => panic!("Expected third step to be filter"),
        }

        match &steps[3] {
            Step::Eval(s) => assert_eq!(s, "third_eval"),
            _ => panic!("Expected fourth step to be eval"),
        }

        println!("✅ Multiple same type test passed");
    }
}
