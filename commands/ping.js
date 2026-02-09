const { SlashCommandBuilder, EmbedBuilder } = require('discord.js');

module.exports = {
    data: new SlashCommandBuilder()
        .setName('ping')
        .setDescription('Replies with Pong!'),
    async execute(interaction) {
        const embed = new EmbedBuilder()
            .setColor(0x00AE86)
            .setTitle('🏓 Pong!')
            .setDescription('Slash command is working!')
            .setTimestamp();

        await interaction.reply({ embeds: [embed] });
    }
};
