FactoryGirl.define do
  factory :redshift_replication, class: 'RailsRedshiftReplicator::Replication' do
    export_format "csv"
    replication_type "IdReplicator"
    key "rdplibrary/replication/users/users_1442958786.csv"
    source_table "users"
  end

  factory :user do
    sequence(:login) { |n| "login_#{n}" }
    sequence(:age)
    factory :user_with_tags do
      transient do
        tags_count 5
      end
      after(:create) do |user, evaluator|
        create_list(:tag, evaluator.tags_count, user: [user])
      end
    end
  end

  factory :post do
    user
    content "text"
  end

  factory :tag do
    sequence(:name) { |n| "tag_#{n}" }
  end
end
