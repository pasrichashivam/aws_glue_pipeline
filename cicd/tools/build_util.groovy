def select_environment(def target_branch){
    switch(target_branch){
        case develop:
            env.ENVIRONMENT="dev"
            echo ("env.ENVIRONMENT": ${env.ENVIRONMENT})
            break
        case production:
            env.ENVIRONMENT="pro"
            echo ("env.ENVIRONMENT": ${env.ENVIRONMENT})
            break
    }
}